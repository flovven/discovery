import assert from 'node:assert/strict';
import test from 'node:test';

import {
  AutoReg,
  bootstrapAutoReg,
  createDiscoveryProvider,
  extractConfigSection,
  isRegistryEnabled,
  resolveHeartbeatMs,
  resolveRegistryType,
} from '../dist/index.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function baseConfig() {
  return {
    discovery: {
      instance: {
        serviceName: 'order-service',
        ip: '10.0.0.5',
        port: 9000,
        metadata: { version: '0.1.0' },
      },
      registries: {
        etcd: { endpoints: ['http://127.0.0.1:2379'] },
        nacos: { serverAddr: '127.0.0.1:8848', namespace: 'public' },
        eureka: { serverUrl: 'http://127.0.0.1:8761/eureka' },
      },
      dependencies: ['user-service'],
    },
  };
}

/**
 * 记录型 mock provider 工厂：把 register/deregister/renew/resolve/close
 * 全部写入 log，行为差异由 overrides 控制。
 */
function recordingFactory(log, overrides = {}) {
  return (ctx) => ({
    name: ctx.type,
    async register(instance) {
      log.push({ op: 'register', registry: ctx.name, instance });
      if (overrides.registerThrows?.(ctx.name)) {
        throw new Error(`register failed: ${ctx.name}`);
      }
    },
    async deregister() {
      log.push({ op: 'deregister', registry: ctx.name });
      if (overrides.deregisterThrows?.(ctx.name)) {
        throw new Error(`deregister failed: ${ctx.name}`);
      }
    },
    async renew() {
      log.push({ op: 'renew', registry: ctx.name });
      if (overrides.renewThrows?.(ctx.name)) {
        throw new Error(`renew failed: ${ctx.name}`);
      }
    },
    async resolve(serviceName) {
      log.push({ op: 'resolve', registry: ctx.name, serviceName });
      if (overrides.resolveThrows?.(ctx.name)) {
        throw new Error(`resolve failed: ${ctx.name}`);
      }
      return overrides.resolve?.(ctx.name, serviceName) ?? [];
    },
    async close() {
      log.push({ op: 'close', registry: ctx.name });
    },
  });
}

const count = (log, op, registry) =>
  log.filter((e) => e.op === op && (registry === undefined || e.registry === registry)).length;

const inst = (serviceName, id, ip, port) => ({
  serviceName,
  id,
  ip,
  port,
  metadata: {},
  weight: 1,
  healthy: true,
});

test('bootstrapAutoReg: 按配置段批量注册到 etcd/nacos/eureka 并返回实例身份', async () => {
  const log = [];
  const app = await bootstrapAutoReg({ config: baseConfig(), factory: recordingFactory(log) });
  try {
    assert.equal(count(log, 'register'), 3);
    assert.deepEqual(log.filter((e) => e.op === 'register').map((e) => e.registry), ['etcd', 'nacos', 'eureka']);
    assert.equal(log[0].instance.id, '10.0.0.5:9000');
    assert.equal(log[0].instance.serviceName, 'order-service');

    const status = app.status();
    assert.equal(status.revision, 1);
    assert.equal(status.registries.length, 3);
    assert.deepEqual(status.registries.map((r) => r.name), ['etcd', 'nacos', 'eureka']);
    for (const r of status.registries) {
      assert.equal(r.registered, true);
      assert.equal(r.heartbeats, 0);
      assert.equal(r.enabled, true);
    }
    assert.deepEqual(app.registryNames(), ['etcd', 'nacos', 'eureka']);
    assert.deepEqual(app.dependencies(), ['user-service']);
    assert.equal(status.failures.length, 0);
  } finally {
    await app.close();
  }
});

test('bootstrapAutoReg: 心跳按 heartbeatMs 周期续约', async () => {
  const log = [];
  const config = baseConfig();
  config.discovery.registries = {
    etcd: { endpoints: ['http://127.0.0.1:2379'], heartbeatMs: 15 },
    nacos: { serverAddr: '127.0.0.1:8848', heartbeatMs: 15 },
  };
  const app = await bootstrapAutoReg({ config, factory: recordingFactory(log) });
  try {
    await sleep(90);
    const renews = log.filter((e) => e.op === 'renew');
    assert.ok(renews.length >= 4, `expected >=4 renews, got ${renews.length}`);
    assert.deepEqual(new Set(renews.map((e) => e.registry)), new Set(['etcd', 'nacos']));
    const status = app.status();
    for (const r of status.registries) {
      assert.ok(r.heartbeats >= 2, `registry ${r.name} heartbeats=${r.heartbeats}`);
    }
  } finally {
    await app.close();
  }
});

test('reload: 配置新增即注册、移除即注销、未变化不动', async () => {
  const log = [];
  const initial = baseConfig();
  initial.discovery.registries = {
    etcd: { endpoints: ['http://127.0.0.1:2379'] },
    nacos: { serverAddr: '127.0.0.1:8848', namespace: 'public' },
  };
  const app = await bootstrapAutoReg({ config: initial, factory: recordingFactory(log) });
  try {
    assert.equal(count(log, 'register', 'etcd'), 1);

    const next = baseConfig();
    next.discovery.registries = {
      // 保留 etcd（配置完全一致）
      etcd: { endpoints: ['http://127.0.0.1:2379'] },
      // 移除 nacos，新增 eureka
      eureka: { serverUrl: 'http://127.0.0.1:8761/eureka' },
    };
    next.discovery.dependencies = ['user-service', 'pay-service'];

    const report = await app.reload(next);
    assert.deepEqual(report.added, ['eureka']);
    assert.deepEqual(report.removed, ['nacos']);
    assert.deepEqual(report.kept, ['etcd']);
    assert.equal(report.revision, 2);
    assert.deepEqual(report.dependencies, ['user-service', 'pay-service']);
    assert.equal(report.ok, true);

    // 被移除的 nacos 注销 + 关闭；etcd 未被重复注册
    assert.equal(count(log, 'deregister', 'nacos'), 1);
    assert.equal(count(log, 'close', 'nacos'), 1);
    assert.equal(count(log, 'register', 'etcd'), 1);
    assert.equal(count(log, 'register', 'eureka'), 1);

    assert.deepEqual(app.registryNames(), ['etcd', 'eureka']);
    assert.deepEqual(
      app.status().registries.map((r) => r.name),
      ['etcd', 'eureka'],
    );
  } finally {
    await app.close();
  }
});

test('reload: 注册中心配置变更触发先注销再重建', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: {
      discovery: {
        instance: { serviceName: 'order-service', ip: '10.0.0.5', port: 9000 },
        registries: { etcd: { endpoints: ['http://127.0.0.1:2379'] } },
      },
    },
    factory: recordingFactory(log),
  });
  try {
    const report = await app.reload({
      discovery: {
        instance: { serviceName: 'order-service', ip: '10.0.0.5', port: 9000 },
        registries: { etcd: { endpoints: ['http://127.0.0.1:2381'] } },
      },
    });
    assert.deepEqual(report.removed, ['etcd']);
    assert.deepEqual(report.added, ['etcd']);
    assert.deepEqual(report.kept, []);
    assert.equal(count(log, 'deregister', 'etcd'), 1);
    assert.equal(count(log, 'register', 'etcd'), 2);
    assert.equal(app.status().revision, 2);
  } finally {
    await app.close();
  }
});

test('失败聚合: 单个注册中心注册失败不阻断其余注册中心', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: baseConfig(),
    factory: recordingFactory(log, { registerThrows: (name) => name === 'nacos' }),
  });
  try {
    assert.deepEqual(app.registryNames(), ['etcd', 'eureka']);
    const status = app.status();
    assert.equal(status.registries.length, 2);
    assert.equal(status.failures.length, 1);
    assert.equal(status.failures[0].registry, 'nacos');
    assert.equal(status.failures[0].phase, 'register');
    assert.match(status.failures[0].message, /register failed: nacos/);
    // 失败源的 provider 已被关闭
    assert.equal(count(log, 'close', 'nacos'), 1);
  } finally {
    await app.close();
  }
});

test('失败聚合: 建连失败与不支持的类型只记录不抛出', async () => {
  const log = [];
  const config = baseConfig();
  config.discovery.registries.consul = { endpoints: ['127.0.0.1:8500'] };
  const factory = (ctx) => {
    if (ctx.name === 'eureka') throw new Error('connect refused');
    return recordingFactory(log)(ctx);
  };
  const app = await bootstrapAutoReg({ config, factory });
  try {
    const failures = app.allFailures();
    assert.equal(failures.length, 2);
    assert.deepEqual(failures.map((f) => f.registry).sort(), ['consul', 'eureka']);
    assert.equal(failures.find((f) => f.registry === 'eureka').phase, 'connect');
    assert.equal(failures.find((f) => f.registry === 'consul').phase, 'connect');
    assert.match(failures.find((f) => f.registry === 'consul').message, /unsupported registry type/);
    assert.deepEqual(app.registryNames(), ['etcd', 'nacos']);
  } finally {
    await app.close();
  }
});

test('心跳失败聚合为 renew 阶段且不影响其他注册中心', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: {
      discovery: {
        instance: { serviceName: 'order-service', ip: '10.0.0.5', port: 9000 },
        registries: {
          etcd: { endpoints: ['http://127.0.0.1:2379'], heartbeatMs: 10 },
          nacos: { serverAddr: '127.0.0.1:8848', heartbeatMs: 10 },
        },
      },
    },
    factory: recordingFactory(log, { renewThrows: (name) => name === 'etcd' }),
  });
  try {
    await sleep(60);
    const status = app.status();
    const etcd = status.registries.find((r) => r.name === 'etcd');
    const nacos = status.registries.find((r) => r.name === 'nacos');
    assert.ok(nacos.heartbeats >= 2, `nacos heartbeats=${nacos.heartbeats}`);
    assert.equal(etcd.heartbeats, 0);
    assert.match(etcd.lastError, /renew failed: etcd/);
    const renewFailures = status.failures.filter((f) => f.phase === 'renew');
    assert.ok(renewFailures.length >= 2);
    assert.equal(renewFailures[0].registry, 'etcd');
  } finally {
    await app.close();
  }
});

test('discover: 跨注册中心合并去重，单源失败跳过', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: baseConfig(),
    factory: recordingFactory(log, {
      resolve: (name) => {
        if (name === 'etcd') {
          return [inst('user-service', 'a', '10.1.0.1', 8080), inst('user-service', 'b', '10.1.0.2', 8080)];
        }
        if (name === 'nacos') {
          return [inst('user-service', 'b', '10.1.0.2', 8080), inst('user-service', 'c', '10.1.0.3', 8080)];
        }
        return [];
      },
      resolveThrows: (name) => name === 'eureka',
    }),
    discoverOnBootstrap: false,
  });
  try {
    const list = await app.discover('user-service');
    assert.deepEqual(list.map((i) => i.id), ['a', 'b', 'c']);
    const failures = app.allFailures().filter((f) => f.phase === 'discover');
    assert.equal(failures.length, 1);
    assert.equal(failures[0].registry, 'eureka');
    assert.equal(failures[0].service, 'user-service');

    const status = app.status();
    assert.equal(status.registries.find((r) => r.name === 'eureka').lastError, 'resolve failed: eureka');
  } finally {
    await app.close();
  }
});

test('bootstrapAutoReg: 按 dependencies 执行发现并缓存到状态', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: baseConfig(),
    factory: recordingFactory(log, {
      resolve: (name) => [inst('dep', `${name}-1`, '10.2.0.1', 8081)],
    }),
  });
  try {
    const deps = app.status().dependencies;
    assert.equal(deps.length, 1);
    assert.equal(deps[0].serviceName, 'user-service');
    assert.deepEqual(deps[0].instances.map((i) => i.id).sort(), ['etcd-1', 'eureka-1', 'nacos-1']);
    assert.equal(deps[0].error, undefined);

    const again = await app.discoverDependencies();
    assert.equal(again[0].instances.length, 3);
  } finally {
    await app.close();
  }
});

test('enabled=false 的注册中心不注册，后续开启可激活', async () => {
  const log = [];
  const config = baseConfig();
  config.discovery.registries = {
    etcd: { endpoints: ['http://127.0.0.1:2379'] },
    nacos: { serverAddr: '127.0.0.1:8848', enabled: false },
  };
  const app = await bootstrapAutoReg({ config, factory: recordingFactory(log) });
  try {
    assert.deepEqual(app.registryNames(), ['etcd']);

    const next = baseConfig();
    next.discovery.registries = {
      etcd: { endpoints: ['http://127.0.0.1:2379'] },
      nacos: { serverAddr: '127.0.0.1:8848', enabled: true },
    };
    const report = await app.reload(next);
    assert.deepEqual(report.added, ['nacos']);
    assert.deepEqual(app.registryNames(), ['etcd', 'nacos']);
  } finally {
    await app.close();
  }
});

test('配置来源支持异步函数，且可直接传 discovery 段本身', async () => {
  const log = [];
  const sectionOnly = {
    instance: { serviceName: 'api-service', ip: '10.0.0.7', port: 7000 },
    registries: { etcd: { endpoints: ['http://127.0.0.1:2379'] } },
  };
  const app = await bootstrapAutoReg({
    config: async () => ({ discovery: sectionOnly }),
    factory: recordingFactory(log),
  });
  try {
    assert.equal(app.currentInstance.serviceName, 'api-service');
    assert.equal(app.currentInstance.id, '10.0.0.7:7000');

    const report = await app.reload(sectionOnly);
    assert.deepEqual(report.kept, ['etcd']);
    assert.equal(report.revision, 2);
  } finally {
    await app.close();
  }
});

test('缺少 config 段时禁用自动注册并记录 config 阶段错误', async () => {
  const log = [];
  const app = await bootstrapAutoReg({ config: { app: { name: 'x' } }, factory: recordingFactory(log) });
  assert.deepEqual(app.registryNames(), []);
  const failures = app.allFailures();
  assert.equal(failures.length, 1);
  assert.equal(failures[0].phase, 'config');
  assert.match(failures[0].message, /no discovery config section/);
  await app.close();
});

test('close: 注销全部注册中心、停止心跳且幂等', async () => {
  const log = [];
  const config = baseConfig();
  config.discovery.registries = {
    etcd: { endpoints: ['http://127.0.0.1:2379'], heartbeatMs: 10 },
    nacos: { serverAddr: '127.0.0.1:8848', heartbeatMs: 10 },
  };
  const app = await AutoReg.bootstrap({ config, factory: recordingFactory(log), discoverOnBootstrap: false });
  await sleep(40);
  await app.close();
  const renewsAtClose = count(log, 'renew');

  assert.deepEqual(log.filter((e) => e.op === 'deregister').map((e) => e.registry), ['etcd', 'nacos']);
  assert.equal(count(log, 'close'), 2);
  assert.deepEqual(app.registryNames(), []);

  await sleep(40);
  assert.equal(count(log, 'renew'), renewsAtClose, 'heartbeat must stop after close');
  assert.ok(renewsAtClose >= 2);

  const second = await app.close();
  assert.deepEqual(second, []);
  assert.deepEqual(app.registryNames(), []);
});

test('close 后 reload 不再生效', async () => {
  const log = [];
  const app = await bootstrapAutoReg({ config: baseConfig(), factory: recordingFactory(log) });
  await app.close();
  const before = log.length;
  const report = await app.reload(baseConfig());
  assert.equal(report.ok, false);
  assert.equal(report.failures[0].phase, 'config');
  assert.equal(log.length, before);
  assert.deepEqual(app.registryNames(), []);
});

test('辅助函数: 段提取 / 类型推断 / 可用性判断 / 心跳解析', () => {
  assert.equal(extractConfigSection({ discovery: { instance: {} } }).instance !== undefined, true);
  assert.equal(extractConfigSection({ instance: {}, registries: {} }) !== null, true);
  assert.equal(extractConfigSection({ other: 1 }), null);
  assert.equal(extractConfigSection([1, 2]), null);

  assert.equal(resolveRegistryType('etcd', {}), 'etcd');
  assert.equal(resolveRegistryType('ETCD', {}), 'etcd');
  assert.equal(resolveRegistryType('primary', { type: 'eureka' }), 'eureka');
  assert.equal(resolveRegistryType('consul', {}), null);

  assert.equal(isRegistryEnabled({ endpoints: ['x'] }, 'etcd'), true);
  assert.equal(isRegistryEnabled({ endpoints: [] }, 'etcd'), false);
  assert.equal(isRegistryEnabled({ endpoints: ['x'], enabled: false }, 'etcd'), false);
  assert.equal(isRegistryEnabled({ server_url: 'http://e/eureka' }, 'eureka'), true);
  assert.equal(isRegistryEnabled({ server_addr: '127.0.0.1:8848' }, 'nacos'), true);
  assert.equal(isRegistryEnabled({}, 'nacos'), false);

  assert.equal(resolveHeartbeatMs({ heartbeatMs: 500 }), 500);
  assert.equal(resolveHeartbeatMs({ heartbeat_interval: 800 }), 800);
  assert.equal(resolveHeartbeatMs({}, 1234), 1234);
});

test('snake_case 别名配置（bufgot 兼容）可正常建连', async () => {
  const log = [];
  const app = await bootstrapAutoReg({
    config: {
      discovery: {
        instance: { serviceName: 'alias-service', ip: '10.0.0.9', port: 9001 },
        registries: {
          eureka: { server_url: 'http://127.0.0.1:8761/eureka', heartbeat_interval: 20 },
          nacos: { server_addr: '127.0.0.1:8848' },
        },
      },
    },
    factory: recordingFactory(log),
  });
  try {
    assert.deepEqual(app.registryNames(), ['eureka', 'nacos']);
    await sleep(60);
    assert.ok(count(log, 'renew', 'eureka') >= 2);
  } finally {
    await app.close();
  }
});

test('缺省工厂按类型创建内置实现且缺地址时抛错被聚合', () => {
  assert.throws(
    () => createDiscoveryProvider({ name: 'etcd', type: 'etcd', config: {}, instance: {}, heartbeatMs: 1000 }),
    /requires endpoints/,
  );
  assert.throws(
    () => createDiscoveryProvider({ name: 'eureka', type: 'eureka', config: {}, instance: {}, heartbeatMs: 0 }),
    /requires serverUrl/,
  );
  assert.throws(
    () => createDiscoveryProvider({ name: 'nacos', type: 'nacos', config: {}, instance: {}, heartbeatMs: 0 }),
    /requires serverAddr/,
  );

  const etcd = createDiscoveryProvider({
    name: 'etcd',
    type: 'etcd',
    config: { endpoints: ['http://127.0.0.1:2379'] },
    instance: {},
    heartbeatMs: 30_000,
  });
  assert.equal(etcd.name, 'etcd');
  const eureka = createDiscoveryProvider({
    name: 'eureka',
    type: 'eureka',
    config: { endpoints: ['http://127.0.0.1:8761/eureka'] },
    instance: {},
    heartbeatMs: 30_000,
  });
  assert.equal(eureka.name, 'eureka');
  const nacos = createDiscoveryProvider({
    name: 'nacos',
    type: 'nacos',
    config: { serverAddr: '127.0.0.1:8848' },
    instance: {},
    heartbeatMs: 30_000,
  });
  assert.equal(nacos.name, 'nacos');
});
