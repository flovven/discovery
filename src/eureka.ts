import type { ResolvedInstance, ServiceDiscovery, ServiceInstance } from './types.js';

export interface EurekaDiscoveryOptions {
  /** eureka server 根地址，如 'http://localhost:8761/eureka' */
  serverUrl: string;
  /** 注册后心跳间隔 ms，默认 30_000（与 eureka 默认续约周期一致） */
  heartbeatMs?: number;
  /** subscribe 轮询间隔 ms，默认 15_000 */
  pollMs?: number;
}

interface EurekaInstanceJson {
  instanceId?: string;
  app?: string;
  hostName?: string;
  ipAddr?: string;
  port?: { $?: number } | number;
  status?: string;
  metadata?: Record<string, string>;
}

interface EurekaResponse {
  applications?: {
    application?: EurekaInstanceJson | EurekaInstanceJson[] | null;
  };
}

/** lib 仅含 ES2022 时 fetch Response 类型不全，用最小接口兜底 */
interface HttpResponseLike {
  status: number;
  statusText: string;
  json(): Promise<unknown>;
}

/**
 * Eureka 服务发现：基于官方 REST API 的自实现客户端（不依赖第三方包）。
 *
 * - register：PUT /apps/{app} 注册 + 定时心跳续约
 * - deregister：DELETE /apps/{app}/{instanceId} 并停止心跳
 * - resolve：GET /apps/{app}（Accept: application/json）
 * - subscribe：轮询拉取，实例列表变化时回调
 */
export class EurekaDiscovery implements ServiceDiscovery {
  readonly name = 'eureka';
  private readonly opts: EurekaDiscoveryOptions;
  private readonly heartbeats = new Map<string, ReturnType<typeof setInterval>>();

  constructor(opts: EurekaDiscoveryOptions) {
    this.opts = opts;
  }

  private base(): string {
    return this.opts.serverUrl.replace(/\/+$/, '');
  }

  private instanceId(instance: ServiceInstance): string {
    return instance.id ?? `${instance.ip}:${instance.port}`;
  }

  private toInstanceJson(instance: ServiceInstance): Record<string, unknown> {
    return {
      instanceId: this.instanceId(instance),
      app: instance.serviceName.toUpperCase(),
      hostName: instance.metadata?.hostname ?? instance.ip,
      ipAddr: instance.ip,
      port: { $: instance.port, '@enabled': 'true' },
      vipAddress: instance.serviceName,
      secureVipAddress: instance.metadata?.secureVipAddress,
      homePageUrl: instance.metadata?.homePageUrl,
      statusPageUrl: instance.metadata?.statusPageUrl,
      healthCheckUrl: instance.metadata?.healthCheckUrl,
      status: 'UP',
      overriddenstatus: 'UNKNOWN',
      dataCenterInfo: {
        name: instance.metadata?.dataCenter ?? 'MyOwn',
        '@class': 'com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo',
      },
      metadata: instance.metadata ?? {},
    };
  }

  private async request(method: string, path: string, body?: unknown): Promise<HttpResponseLike> {
    const res = (await fetch(`${this.base()}${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    })) as unknown as HttpResponseLike;
    if (res.status >= 400) {
      throw new Error(`eureka ${method} ${path} failed: ${res.status} ${res.statusText}`);
    }
    return res;
  }

  async register(instance: ServiceInstance): Promise<void> {
    const id = this.instanceId(instance);
    await this.request('PUT', `/apps/${instance.serviceName.toUpperCase()}`, this.toInstanceJson(instance));
    // 心跳续约；重复注册时先清旧定时器
    const prev = this.heartbeats.get(id);
    if (prev) {
      clearInterval(prev);
    }
    const timer = setInterval(() => {
      void this.request('PUT', `/apps/${instance.serviceName.toUpperCase()}/${encodeURIComponent(id)}`).catch(
        () => {
          /* 心跳失败由 eureka 侧超时摘除，忽略 */
        },
      );
    }, this.opts.heartbeatMs ?? 30_000);
    this.heartbeats.set(id, timer);
  }

  async deregister(instance: ServiceInstance): Promise<void> {
    const id = this.instanceId(instance);
    await this.request('DELETE', `/apps/${instance.serviceName.toUpperCase()}/${encodeURIComponent(id)}`).catch(
      () => {
        /* 注销 404 视为已下线 */
      },
    );
    const timer = this.heartbeats.get(id);
    if (timer) {
      clearInterval(timer);
      this.heartbeats.delete(id);
    }
  }

  async resolve(serviceName: string): Promise<ResolvedInstance[]> {
    const res = await this.request('GET', `/apps/${serviceName.toUpperCase()}`);
    const data = (await res.json()) as EurekaResponse;
    const app = data.applications?.application;
    const list = Array.isArray(app) ? app : app ? [app] : [];
    return list.map((a) => ({
      serviceName,
      id: a.instanceId ?? `${a.ipAddr ?? ''}:${typeof a.port === 'object' ? a.port?.$ ?? 0 : a.port ?? 0}`,
      ip: a.ipAddr ?? a.hostName ?? '',
      port: typeof a.port === 'object' ? a.port?.$ ?? 0 : a.port ?? 0,
      metadata: a.metadata ?? {},
      weight: 1,
      healthy: (a.status ?? '').toUpperCase() === 'UP',
    }));
  }

  /** 轮询订阅：实例列表变化时回调 */
  async subscribe(serviceName: string, listener: (instances: ResolvedInstance[]) => void): Promise<() => void> {
    let last = '';
    const tick = async () => {
      try {
        const list = await this.resolve(serviceName);
        const sig = JSON.stringify(list);
        if (sig !== last) {
          last = sig;
          listener(list);
        }
      } catch {
        /* 轮询失败跳过，下轮再试 */
      }
    };
    await tick();
    const timer = setInterval(() => void tick(), this.opts.pollMs ?? 15_000);
    return () => clearInterval(timer);
  }
}
