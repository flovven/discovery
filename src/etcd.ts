import type { ResolvedInstance, ServiceDiscovery, ServiceInstance } from './types.js';
import type { Etcd3, Lease } from 'etcd3';

export interface EtcdDiscoveryOptions {
  /** etcd 连接地址，单点 'http://127.0.0.1:2379' 或集群数组 */
  hosts: string | string[];
  /** 服务注册键前缀，默认 '/flovven/discovery' */
  prefix?: string;
  /** 租约 TTL（秒），默认 30s，注册实例按此续约 */
  ttlSeconds?: number;
}

const defaultPrefix = '/flovven/discovery';

/**
 * Etcd 服务发现：基于 etcd3 的 key-value + 租约。
 *
 * 存储结构：{prefix}/{serviceName}/{instanceId} -> JSON(ResolvedInstance)
 *  - register：写入 value 并挂租约（自动续约），过期自动摘除
 *  - deregister：删除 key 并撤销对应租约
 *  - resolve：按服务名前缀 getAll
 *  - subscribe：watch 该服务前缀，任何 put/delete 触发重新拉取
 */
export class EtcdDiscovery implements ServiceDiscovery {
  readonly name = 'etcd';
  private readonly opts: EtcdDiscoveryOptions;
  private client: Etcd3 | null = null;
  private readonly leases = new Map<string, Lease>();
  private readonly prefixOf = (serviceName: string) => `${this.opts.prefix ?? defaultPrefix}/${serviceName}`;
  private readonly keyOf = (serviceName: string, id: string) => `${this.prefixOf(serviceName)}/${id}`;

  constructor(opts: EtcdDiscoveryOptions) {
    this.opts = opts;
  }

  private async getClient(): Promise<Etcd3> {
    if (!this.client) {
      const { Etcd3 } = await import('etcd3');
      this.client = new Etcd3({ hosts: this.opts.hosts });
    }
    return this.client;
  }

  private instanceId(instance: ServiceInstance): string {
    return instance.id ?? `${instance.ip}:${instance.port}`;
  }

  async register(instance: ServiceInstance): Promise<void> {
    const c = await this.getClient();
    const id = this.instanceId(instance);
    const key = this.keyOf(instance.serviceName, id);
    // 每次注册挂新租约（含自动续约），避免旧租约残留
    const lease = c.lease(this.opts.ttlSeconds ?? 30);
    await lease.put(key).value(
      JSON.stringify({
        serviceName: instance.serviceName,
        id,
        ip: instance.ip,
        port: instance.port,
        metadata: instance.metadata ?? {},
        weight: instance.weight ?? 1,
        healthy: true,
      }),
    );
    const prev = this.leases.get(key);
    if (prev) {
      try {
        await prev.revoke();
      } catch {
        /* 旧租约可能已过期，忽略 */
      }
    }
    this.leases.set(key, lease);
  }

  async deregister(instance: ServiceInstance): Promise<void> {
    const c = await this.getClient();
    const key = this.keyOf(instance.serviceName, this.instanceId(instance));
    await c.delete().key(key);
    const lease = this.leases.get(key);
    if (lease) {
      try {
        await lease.revoke();
      } catch {
        /* 忽略 */
      }
      this.leases.delete(key);
    }
  }

  async resolve(serviceName: string): Promise<ResolvedInstance[]> {
    const c = await this.getClient();
    const all = await c.getAll().prefix(this.prefixOf(serviceName)).strings();
    const out: ResolvedInstance[] = [];
    for (const raw of Object.values(all)) {
      try {
        const parsed = JSON.parse(String(raw)) as ResolvedInstance;
        parsed.serviceName = serviceName;
        out.push(parsed);
      } catch {
        /* 脏数据跳过 */
      }
    }
    return out;
  }

  async subscribe(serviceName: string, listener: (instances: ResolvedInstance[]) => void): Promise<() => void> {
    const c = await this.getClient();
    const watcher = await c.watch().prefix(this.prefixOf(serviceName)).create();
    const handler = () => {
      void this.resolve(serviceName).then(listener);
    };
    watcher.on('put', handler);
    watcher.on('delete', handler);
    return () => {
      watcher.removeListener('put', handler);
      watcher.removeListener('delete', handler);
      void watcher.cancel();
    };
  }
}
