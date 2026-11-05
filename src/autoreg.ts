/**
 * 配置驱动的服务自动注册 —— @flovven/discovery
 *
 * 对标 bufgot/discovery/autoreg（BootstrapAutoReg + App）与 ferrite-discovery 的 autoreg：
 * 从配置对象中识别 `discovery` 段的多个注册中心（etcd / eureka / nacos），
 * 批量建立连接、注册本机实例、启动心跳续约，并按 `dependencies` 列表执行服务发现；
 * 配置变更时（如配置中心推送新增/移除某个注册中心）动态增删：
 *   - 新增段   → 建连 + 注册 + 起心跳
 *   - 移除段   → 注销 + 停心跳 + 关闭连接
 *   - 修改段   → 先按旧配置注销，再按新配置重建
 *
 * 设计要点：
 *  - 单源失败不阻断：任一注册中心建连/注册/续约/发现失败只记录到 `failures`，
 *    其余注册中心照常工作，入口函数不会因此抛出。
 *  - 统一入口：`bootstrapAutoReg(options)` 一行完成"读配置 → 注册 → 心跳 → 发现"。
 *  - 状态查询：`app.status()` 返回实例、各注册中心注册态、心跳次数、依赖发现结果与错误明细。
 *  - 可注入：`options.factory` 可替换 provider 工厂（测试注入 mock，业务侧可自定义实现）。
 */
import { EtcdDiscovery } from './etcd.js';
import { EurekaDiscovery } from './eureka.js';
import { NacosDiscovery } from './nacos.js';
import type { ResolvedInstance, ServiceDiscovery, ServiceInstance } from './types.js';

/** 支持的注册中心类型 */
export type RegistryType = 'etcd' | 'eureka' | 'nacos';

const REGISTRY_TYPES: readonly RegistryType[] = ['etcd', 'eureka', 'nacos'];

/** 默认心跳/续约间隔：30s（与 eureka 默认续约周期、etcd 默认租约 TTL 对齐） */
export const DEFAULT_HEARTBEAT_MS = 30_000;

/** 配置段默认键名（与 bufgot 的 `BootstrapAutoReg(app, nil, "discovery")` 对齐） */
export const DEFAULT_CONFIG_KEY = 'discovery';

/** 单个注册中心的配置段 */
export interface RegistryConfig {
  /** 注册中心类型；缺省时按配置段键名推断（`registries.etcd` → etcd） */
  type?: RegistryType;
  /** 是否启用，默认 true；false 等同移除 */
  enabled?: boolean;
  /** 地址列表：etcd 为 hosts；eureka/nacos 缺省取首项作为 serverUrl / serverAddr */
  endpoints?: string[];
  /** eureka server 根地址，如 http://127.0.0.1:8761/eureka */
  serverUrl?: string;
  /** eureka server 根地址（snake_case 别名，兼容 bufgot 配置） */
  server_url?: string;
  /** nacos 地址，如 127.0.0.1:8848 */
  serverAddr?: string;
  /** nacos 地址（snake_case 别名） */
  server_addr?: string;
  /** nacos 命名空间 */
  namespace?: string;
  /** nacos 分组（保留字段：现有 NacosDiscovery 以 serviceName 维度注册，分组透传到元数据） */
  group?: string;
  /** etcd 键前缀，默认 /flovven/discovery */
  prefix?: string;
  /** etcd 租约 TTL（秒）；缺省按 heartbeatMs 换算 */
  ttlSeconds?: number;
  /** 心跳/续约间隔 ms；<=0 表示不启用框架层心跳（交由 provider 自身续约） */
  heartbeatMs?: number;
  /** 心跳间隔 ms（snake_case 别名） */
  heartbeat_interval?: number;
}

/** 本机服务实例身份配置 */
export interface AutoRegInstanceConfig {
  serviceName: string;
  ip: string;
  port: number;
  /** 实例 ID，缺省按 ip:port 生成 */
  id?: string;
  metadata?: Record<string, string>;
  weight?: number;
  ephemeral?: boolean;
}

/** `discovery` 段配置 */
export interface AutoRegConfig {
  instance?: AutoRegInstanceConfig;
  /** 注册中心段：键名为注册中心别名（type 缺省时用作类型） */
  registries?: Record<string, RegistryConfig>;
  /** 需要发现的依赖服务名列表 */
  dependencies?: string[];
}

/** 配置来源：对象、或返回配置的同步/异步函数（便于配置中心变更时重读） */
export type AutoRegConfigSource = unknown | (() => unknown | Promise<unknown>);

/** provider 工厂上下文 */
export interface RegistryFactoryContext {
  /** 注册中心别名（配置段键名） */
  name: string;
  type: RegistryType;
  config: RegistryConfig;
  instance: ServiceInstance;
  heartbeatMs: number;
}

/** provider 能力：除标准 ServiceDiscovery 外，可选 renew / close */
export interface RegistryProvider extends ServiceDiscovery {
  /** 心跳续约；缺省时框架层心跳退化为再次 register */
  renew?(instance: ServiceInstance): Promise<void>;
  /** 释放底层连接与定时器 */
  close?(): Promise<void> | void;
}

/** provider 工厂 */
export type RegistryFactory = (ctx: RegistryFactoryContext) => RegistryProvider | Promise<RegistryProvider>;

/** 错误阶段 */
export type AutoRegPhase = 'config' | 'connect' | 'register' | 'deregister' | 'renew' | 'discover';

/** 单条错误明细 */
export interface AutoRegFailure {
  /** 注册中心别名；配置级错误时为空串 */
  registry: string;
  phase: AutoRegPhase;
  message: string;
  /** 关联服务名（discover 阶段） */
  service?: string;
  cause?: unknown;
}

/** 单个注册中心的运行时状态 */
export interface RegistryStatus {
  name: string;
  type: RegistryType;
  enabled: boolean;
  /** 已成功注册 */
  registered: boolean;
  /** 已执行心跳次数 */
  heartbeats: number;
  /** 最近一次错误信息 */
  lastError?: string;
}

/** 依赖发现结果 */
export interface DependencyStatus {
  serviceName: string;
  instances: ResolvedInstance[];
  error?: string;
}

/** 整体状态快照 */
export interface AutoRegStatus {
  instance: ServiceInstance | null;
  /** 配置版本号，每次 reload 成功应用后 +1 */
  revision: number;
  registries: RegistryStatus[];
  dependencies: DependencyStatus[];
  failures: AutoRegFailure[];
}

/** reload 结果报告 */
export interface AutoRegReloadReport {
  /** 配置未变化/不可用时为当前版本号 */
  revision: number;
  added: string[];
  removed: string[];
  /** 配置未变化而保留的注册中心 */
  kept: string[];
  dependencies: string[];
  failures: AutoRegFailure[];
  /** 是否完全成功（无 failures） */
  ok: boolean;
}

/** 日志接口（缺省静默） */
export interface AutoRegLogger {
  info?(msg: string, meta?: unknown): void;
  warn?(msg: string, meta?: unknown): void;
  error?(msg: string, meta?: unknown): void;
}

/** 自动注册入口选项 */
export interface AutoRegOptions {
  /** 配置来源 */
  config: AutoRegConfigSource;
  /** 配置段键名，默认 `discovery`；段不存在时把 config 本身当作段 */
  configKey?: string;
  /** 显式指定本机实例（优先于配置中的 instance） */
  instance?: AutoRegInstanceConfig;
  /** 自定义 provider 工厂；缺省按 type 创建内置实现 */
  factory?: RegistryFactory;
  /** 默认心跳间隔 ms，默认 30000 */
  heartbeatMs?: number;
  /** bootstrap 后是否立即按 dependencies 执行发现，默认 true */
  discoverOnBootstrap?: boolean;
  /** 错误回调（每条 failures 追加时触发） */
  onFailure?: (failure: AutoRegFailure) => void;
  logger?: AutoRegLogger;
}

interface Entry {
  name: string;
  type: RegistryType;
  config: RegistryConfig;
  signature: string;
  provider: RegistryProvider;
  timer: ReturnType<typeof setInterval> | null;
  heartbeatMs: number;
  heartbeats: number;
  registered: boolean;
  renewing: boolean;
  lastError?: string;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

/** 从完整配置对象中取出 `discovery` 段（不存在时把入参本身当段） */
export function extractConfigSection(raw: unknown, configKey: string = DEFAULT_CONFIG_KEY): AutoRegConfig | null {
  if (!isPlainObject(raw)) return null;
  const nested = raw[configKey];
  if (isPlainObject(nested)) return nested as AutoRegConfig;
  if (isPlainObject(raw.registries) || isPlainObject(raw.instance)) return raw as AutoRegConfig;
  return null;
}

/** 注册中心类型归一：显式 type 优先，其次按配置段键名推断 */
export function resolveRegistryType(name: string, config: RegistryConfig): RegistryType | null {
  const raw = (config.type ?? name).toLowerCase().trim();
  return (REGISTRY_TYPES as readonly string[]).includes(raw) ? (raw as RegistryType) : null;
}

/** 注册中心是否具备连接条件 */
export function isRegistryEnabled(config: RegistryConfig, type: RegistryType): boolean {
  if (config.enabled === false) return false;
  const endpoints = config.endpoints ?? [];
  switch (type) {
    case 'etcd':
      return endpoints.length > 0;
    case 'eureka':
      return Boolean(config.serverUrl ?? config.server_url ?? endpoints[0]);
    case 'nacos':
      return Boolean(config.serverAddr ?? config.server_addr ?? endpoints[0]);
    default:
      return false;
  }
}

/** 心跳间隔：段配置 > 全局默认 */
export function resolveHeartbeatMs(config: RegistryConfig, fallbackMs: number = DEFAULT_HEARTBEAT_MS): number {
  const own = config.heartbeatMs ?? config.heartbeat_interval;
  if (typeof own === 'number' && Number.isFinite(own)) return own;
  return fallbackMs;
}

function makeSignature(config: RegistryConfig): string {
  return JSON.stringify({
    type: config.type ?? '',
    enabled: config.enabled ?? true,
    endpoints: config.endpoints ?? [],
    serverUrl: config.serverUrl ?? config.server_url ?? '',
    serverAddr: config.serverAddr ?? config.server_addr ?? '',
    namespace: config.namespace ?? '',
    group: config.group ?? '',
    prefix: config.prefix ?? '',
    ttlSeconds: config.ttlSeconds ?? 0,
    heartbeatMs: config.heartbeatMs ?? config.heartbeat_interval ?? 0,
  });
}

/**
 * 缺省 provider 工厂：按 type 创建内置实现。
 *
 * - etcd：`EtcdDiscovery`，hosts 取 `endpoints`，租约 TTL 默认 `heartbeatMs * 2 / 1000`
 * - eureka：`EurekaDiscovery`，serverUrl 取 `serverUrl` 或 `endpoints[0]`
 * - nacos：`NacosDiscovery`，serverAddr 取 `serverAddr` 或 `endpoints[0]`
 */
export function createDiscoveryProvider(ctx: RegistryFactoryContext): RegistryProvider {
  const { name, type, config, heartbeatMs } = ctx;
  const endpoints = config.endpoints ?? [];
  switch (type) {
    case 'etcd': {
      if (endpoints.length === 0) {
        throw new Error(`autoreg: registry "${name}" (etcd) requires endpoints`);
      }
      const ttlSeconds =
        typeof config.ttlSeconds === 'number' && config.ttlSeconds > 0
          ? config.ttlSeconds
          : Math.max(1, Math.round((heartbeatMs * 2) / 1000));
      return new EtcdDiscovery({
        hosts: endpoints,
        ...(config.prefix ? { prefix: config.prefix } : {}),
        ttlSeconds,
      });
    }
    case 'eureka': {
      const serverUrl = config.serverUrl ?? config.server_url ?? endpoints[0];
      if (!serverUrl) {
        throw new Error(`autoreg: registry "${name}" (eureka) requires serverUrl or endpoints`);
      }
      return new EurekaDiscovery({
        serverUrl,
        ...(heartbeatMs > 0 ? { heartbeatMs } : {}),
      });
    }
    case 'nacos': {
      const serverAddr = config.serverAddr ?? config.server_addr ?? endpoints[0];
      if (!serverAddr) {
        throw new Error(`autoreg: registry "${name}" (nacos) requires serverAddr or endpoints`);
      }
      return new NacosDiscovery({
        serverAddr,
        ...(config.namespace ? { namespace: config.namespace } : {}),
      });
    }
    default:
      throw new Error(`autoreg: unsupported registry type "${String(type)}"`);
  }
}

function normalizeInstance(cfg: AutoRegInstanceConfig): ServiceInstance {
  const id = cfg.id && cfg.id.trim() !== '' ? cfg.id : `${cfg.ip}:${cfg.port}`;
  return {
    serviceName: cfg.serviceName,
    id,
    ip: cfg.ip,
    port: cfg.port,
    ...(cfg.metadata ? { metadata: { ...cfg.metadata } } : {}),
    ...(typeof cfg.weight === 'number' ? { weight: cfg.weight } : {}),
    ...(typeof cfg.ephemeral === 'boolean' ? { ephemeral: cfg.ephemeral } : {}),
  };
}

const MAX_FAILURES = 100;

/**
 * 自动注册应用：持有多个注册中心的 provider、心跳定时器与配置版本。
 *
 * 生命周期：`bootstrapAutoReg()` → `reload()` 应对配置变更 → `close()` 优雅下线。
 */
export class AutoReg {
  private readonly opts: AutoRegOptions;
  private readonly defaultHeartbeatMs: number;
  private readonly entries = new Map<string, Entry>();
  private readonly failures: AutoRegFailure[] = [];
  private instance: ServiceInstance | null = null;
  private deps: string[] = [];
  private dependencyStatus: DependencyStatus[] = [];
  private revision = 0;
  private closed = false;

  constructor(opts: AutoRegOptions) {
    this.opts = opts;
    this.defaultHeartbeatMs = opts.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
  }

  /** 统一入口：读配置 → 建连注册 → 起心跳 → 按依赖发现（失败不抛出，记入 failures） */
  static async bootstrap(opts: AutoRegOptions): Promise<AutoReg> {
    const app = new AutoReg(opts);
    await app.reload();
    if (opts.discoverOnBootstrap !== false) {
      await app.discoverDependencies();
    }
    return app;
  }

  /** 当前本机实例 */
  get currentInstance(): ServiceInstance | null {
    return this.instance ? { ...this.instance } : null;
  }

  /** 当前配置版本号 */
  get currentRevision(): number {
    return this.revision;
  }

  /** 当前生效的注册中心别名 */
  registryNames(): string[] {
    return [...this.entries.keys()];
  }

  /** 当前依赖服务名列表 */
  dependencies(): string[] {
    return [...this.deps];
  }

  /** 累计错误明细（浅拷贝） */
  allFailures(): AutoRegFailure[] {
    return this.failures.map((f) => ({ ...f }));
  }

  /** 清空错误明细 */
  clearFailures(): void {
    this.failures.length = 0;
  }

  /**
   * 重新读取配置并按差异应用：
   * 新增段建连注册、移除/禁用段注销并停心跳、修改段先注销再重建。
   *
   * @param input 覆盖配置来源（缺省用 options.config）
   */
  async reload(input?: AutoRegConfigSource): Promise<AutoRegReloadReport> {
    const source = input ?? this.opts.config;
    const failuresBefore = this.failures.length;
    const report: AutoRegReloadReport = {
      revision: this.revision,
      added: [],
      removed: [],
      kept: [],
      dependencies: [],
      failures: [],
      ok: true,
    };

    if (this.closed) {
      this.record('', 'config', 'autoreg: application already closed');
      return this.finishReport(report, failuresBefore);
    }

    let raw: unknown;
    try {
      raw = typeof source === 'function' ? await source() : source;
    } catch (err) {
      this.record('', 'config', `autoreg: read config: ${errorMessage(err)}`, undefined, err);
      return this.finishReport(report, failuresBefore);
    }

    const section = extractConfigSection(raw, this.opts.configKey ?? DEFAULT_CONFIG_KEY);
    if (!section) {
      this.record('', 'config', 'autoreg: no discovery config section found, auto-registration disabled');
      return this.finishReport(report, failuresBefore);
    }

    const instanceCfg = this.opts.instance ?? section.instance;
    if (!instanceCfg || !instanceCfg.serviceName || !instanceCfg.ip) {
      this.record('', 'config', 'autoreg: instance config (serviceName/ip/port) is required');
      return this.finishReport(report, failuresBefore);
    }
    this.instance = normalizeInstance(instanceCfg);

    const registryCfgs = section.registries ?? {};
    const nextDeps = Array.isArray(section.dependencies) ? section.dependencies.slice() : [];

    // 1) 移除：配置中已不存在、被禁用，或类型无法识别
    for (const name of [...this.entries.keys()]) {
      const cfg = registryCfgs[name];
      const type = cfg ? resolveRegistryType(name, cfg) : null;
      const keep = cfg !== undefined && type !== null && isRegistryEnabled(cfg, type);
      if (!keep) {
        await this.removeEntry(name, cfg ? 'disabled or invalid' : 'absent from config');
        report.removed.push(name);
      }
    }

    // 2) 新增或修改
    for (const [name, cfg] of Object.entries(registryCfgs)) {
      const type = resolveRegistryType(name, cfg);
      if (type === null) {
        this.record(name, 'connect', `autoreg: unsupported registry type "${String(cfg.type ?? name)}"`);
        continue;
      }
      if (!isRegistryEnabled(cfg, type)) {
        continue;
      }
      const signature = makeSignature(cfg);
      const existing = this.entries.get(name);
      if (existing && existing.signature === signature) {
        report.kept.push(name);
        continue;
      }
      if (existing) {
        await this.removeEntry(name, 'config changed');
        report.removed.push(name);
      }
      const created = await this.addEntry(name, type, cfg, signature);
      if (created) report.added.push(name);
    }

    this.deps = nextDeps;
    report.dependencies = [...this.deps];
    this.revision += 1;
    report.revision = this.revision;
    return this.finishReport(report, failuresBefore);
  }

  /** 跨注册中心发现服务：结果按 id 去重合并，单源失败跳过 */
  async discover(serviceName: string): Promise<ResolvedInstance[]> {
    const seen = new Set<string>();
    const merged: ResolvedInstance[] = [];
    for (const entry of this.entries.values()) {
      try {
        const list = await entry.provider.resolve(serviceName);
        for (const inst of list) {
          const key = `${inst.serviceName}#${inst.id}`;
          if (seen.has(key)) continue;
          seen.add(key);
          merged.push(inst);
        }
      } catch (err) {
        entry.lastError = errorMessage(err);
        this.record(entry.name, 'discover', errorMessage(err), serviceName, err);
      }
    }
    return merged;
  }

  /** 按依赖列表执行发现，结果缓存到 status().dependencies */
  async discoverDependencies(): Promise<DependencyStatus[]> {
    const out: DependencyStatus[] = [];
    for (const svc of this.deps) {
      const before = this.failures.length;
      const instances = await this.discover(svc);
      const errs = this.failures.slice(before);
      out.push({
        serviceName: svc,
        instances,
        ...(errs.length > 0 ? { error: errs.map((e) => `${e.registry}: ${e.message}`).join('; ') } : {}),
      });
    }
    this.dependencyStatus = out;
    return out.map((d) => ({ ...d, instances: d.instances.slice() }));
  }

  /** 状态快照 */
  status(): AutoRegStatus {
    return {
      instance: this.instance ? { ...this.instance } : null,
      revision: this.revision,
      registries: [...this.entries.entries()].map(([name, e]) => ({
        name,
        type: e.type,
        enabled: isRegistryEnabled(e.config, e.type),
        registered: e.registered,
        heartbeats: e.heartbeats,
        ...(e.lastError ? { lastError: e.lastError } : {}),
      })),
      dependencies: this.dependencyStatus.map((d) => ({ ...d, instances: d.instances.slice() })),
      failures: this.failures.map((f) => ({ ...f })),
    };
  }

  /** 优雅下线：注销全部注册中心、停心跳、关闭连接 */
  async close(): Promise<AutoRegFailure[]> {
    if (this.closed) return [];
    this.closed = true;
    const before = this.failures.length;
    for (const name of [...this.entries.keys()]) {
      await this.removeEntry(name, 'close');
    }
    this.deps = [];
    this.dependencyStatus = [];
    return this.failures.slice(before);
  }

  // ── 内部实现 ──────────────────────────────────────────────

  private async addEntry(
    name: string,
    type: RegistryType,
    config: RegistryConfig,
    signature: string,
  ): Promise<boolean> {
    if (!this.instance) return false;
    const instance = this.instance;
    const heartbeatMs = resolveHeartbeatMs(config, this.defaultHeartbeatMs);
    const factory = this.opts.factory ?? createDiscoveryProvider;

    let provider: RegistryProvider;
    try {
      provider = await factory({ name, type, config, instance, heartbeatMs });
    } catch (err) {
      this.record(name, 'connect', `autoreg: create provider: ${errorMessage(err)}`, undefined, err);
      return false;
    }

    try {
      await provider.register(instance);
    } catch (err) {
      this.record(name, 'register', `autoreg: register: ${errorMessage(err)}`, undefined, err);
      try {
        await provider.close?.();
      } catch {
        /* 关闭失败忽略 */
      }
      return false;
    }

    const entry: Entry = {
      name,
      type,
      config,
      signature,
      provider,
      timer: null,
      heartbeatMs,
      heartbeats: 0,
      registered: true,
      renewing: false,
    };
    if (heartbeatMs > 0) {
      entry.timer = setInterval(() => {
        void this.heartbeat(name);
      }, heartbeatMs);
      // 定时器不阻塞进程退出（Node 侧 unref 可选实现）
      const maybeUnref = entry.timer as unknown as { unref?: () => void };
      if (typeof maybeUnref.unref === 'function') maybeUnref.unref();
    }
    this.entries.set(name, entry);
    this.opts.logger?.info?.(`autoreg: registered ${instance.serviceName}/${String(instance.id)} to ${name}`);
    return true;
  }

  private async removeEntry(name: string, reason: string): Promise<void> {
    const entry = this.entries.get(name);
    if (!entry) return;
    // 先摘除，避免心跳重入
    this.entries.delete(name);
    if (entry.timer) {
      clearInterval(entry.timer);
      entry.timer = null;
    }
    if (this.instance) {
      try {
        await entry.provider.deregister(this.instance);
      } catch (err) {
        this.record(name, 'deregister', `autoreg: deregister (${reason}): ${errorMessage(err)}`, undefined, err);
      }
    }
    try {
      await entry.provider.close?.();
    } catch (err) {
      this.record(name, 'connect', `autoreg: close provider (${reason}): ${errorMessage(err)}`, undefined, err);
    }
    entry.registered = false;
    this.opts.logger?.info?.(`autoreg: removed registry ${name} (${reason})`);
  }

  private async heartbeat(name: string): Promise<void> {
    const entry = this.entries.get(name);
    if (!entry || entry.renewing || this.closed || !this.instance) return;
    entry.renewing = true;
    try {
      if (typeof entry.provider.renew === 'function') {
        await entry.provider.renew(this.instance);
      } else {
        await entry.provider.register(this.instance);
      }
      entry.heartbeats += 1;
      entry.registered = true;
    } catch (err) {
      entry.lastError = errorMessage(err);
      this.record(name, 'renew', `autoreg: heartbeat: ${errorMessage(err)}`, undefined, err);
    } finally {
      entry.renewing = false;
    }
  }

  private record(
    registry: string,
    phase: AutoRegPhase,
    message: string,
    service?: string,
    cause?: unknown,
  ): void {
    const failure: AutoRegFailure = {
      registry,
      phase,
      message,
      ...(service !== undefined ? { service } : {}),
      ...(cause !== undefined ? { cause } : {}),
    };
    this.failures.push(failure);
    if (this.failures.length > MAX_FAILURES) {
      this.failures.splice(0, this.failures.length - MAX_FAILURES);
    }
    this.opts.logger?.warn?.(message, failure);
    this.opts.onFailure?.(failure);
  }

  private finishReport(report: AutoRegReloadReport, failuresBefore: number): AutoRegReloadReport {
    report.failures = this.failures.slice(failuresBefore).map((f) => ({ ...f }));
    report.ok = report.failures.length === 0;
    return report;
  }
}

/**
 * 统一入口：一行完成配置驱动的自动注册。
 *
 * ```ts
 * const app = await bootstrapAutoReg({ config: () => cfg.snapshot() });
 * const list = await app.discover('user-service');
 * await app.reload();       // 配置中心推送后重新对账（新增即注册、移除即注销）
 * await app.close();        // 优雅下线
 * ```
 */
export async function bootstrapAutoReg(opts: AutoRegOptions): Promise<AutoReg> {
  return AutoReg.bootstrap(opts);
}
