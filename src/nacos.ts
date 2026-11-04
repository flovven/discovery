import type { ResolvedInstance, ServiceDiscovery, ServiceInstance } from './types.js';

export interface NacosDiscoveryOptions {
  serverAddr: string;
  namespace?: string;
}

type NacosNamingClient = import('nacos-naming').NacosNamingClient;
type NacosHost = import('nacos-naming').Host;
type NacosInstance = import('nacos-naming').Instance;

/** Nacos 鏈嶅姟鍙戠幇锛氭敞鍐?/ 娉ㄩ攢 / 鏌ヨ / 璁㈤槄鍙樻洿 */
export class NacosDiscovery implements ServiceDiscovery {
  readonly name = 'nacos';
  private readonly opts: NacosDiscoveryOptions;
  private nacos: NacosNamingClient | null = null;

  constructor(opts: NacosDiscoveryOptions) {
    this.opts = opts;
  }

  private async client(): Promise<NacosNamingClient> {
    if (!this.nacos) {
      const { NacosNamingClient } = await import('nacos-naming');
      this.nacos = new NacosNamingClient({
        logger: console,
        serverList: this.opts.serverAddr,
        namespace: this.opts.namespace ?? '',
      });
    }
    return this.nacos;
  }

  private toRegister(instance: ServiceInstance): NacosInstance {
    return {
      instanceId: instance.id ?? `${instance.ip}:${instance.port}`,
      ip: instance.ip,
      port: instance.port,
      weight: instance.weight ?? 1,
      ephemeral: instance.ephemeral ?? true,
      healthy: true,
      enabled: true,
    };
  }

  async register(instance: ServiceInstance): Promise<void> {
    const c = await this.client();
    await c.registerInstance(instance.serviceName, this.toRegister(instance));
  }

  async deregister(instance: ServiceInstance): Promise<void> {
    const c = await this.client();
    await c.deregisterInstance(instance.serviceName, this.toRegister(instance));
  }

  async resolve(serviceName: string): Promise<ResolvedInstance[]> {
    const c = await this.client();
    const hosts = await c.getAllInstances(serviceName);
    return hosts.map((h: NacosHost) => ({
      serviceName,
      id: h.instanceId ?? `${h.ip}:${h.port}`,
      ip: h.ip,
      port: h.port,
      metadata: h.metadata ?? {},
      weight: h.weight ?? 1,
      healthy: h.healthy ?? true,
    }));
  }

  async subscribe(serviceName: string, listener: (instances: ResolvedInstance[]) => void): Promise<() => void> {
    const c = await this.client();
    const handler = (hosts: NacosHost[]) => {
      listener(
        hosts.map((h: NacosHost) => ({
          serviceName,
          id: h.instanceId ?? `${h.ip}:${h.port}`,
          ip: h.ip,
          port: h.port,
          metadata: h.metadata ?? {},
          weight: h.weight ?? 1,
          healthy: h.healthy ?? true,
        })),
      );
    };
    c.subscribe(serviceName, handler);
    return () => c.unSubscribe(serviceName, handler);
  }
}
