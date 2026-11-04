/** 待注册的服务实例信息 */
export interface ServiceInstance {
  /** 服务名（注册中心侧的服务标识，如 nacos serviceName / eureka app / etcd 服务目录） */
  serviceName: string;
  /** 实例 ID；缺省时由各实现按 ip:port 生成 */
  id?: string;
  ip: string;
  port: number;
  /** 可选元数据，如版本号、分组 */
  metadata?: Record<string, string>;
  /** 权重，默认 1 */
  weight?: number;
  /** 是否临时实例，默认 true（临时实例走心跳续约） */
  ephemeral?: boolean;
}

/** 解析出的健康实例 */
export interface ResolvedInstance {
  serviceName: string;
  id: string;
  ip: string;
  port: number;
  metadata: Record<string, string>;
  weight: number;
  healthy: boolean;
}

/** 服务发现统一接口：注册 / 注销 / 查询 / 订阅变更 */
export interface ServiceDiscovery {
  readonly name: string;
  /** 注册服务实例（内部自动心跳续约） */
  register(instance: ServiceInstance): Promise<void>;
  /** 注销服务实例 */
  deregister(instance: ServiceInstance): Promise<void>;
  /** 查询某服务的全部健康实例 */
  resolve(serviceName: string): Promise<ResolvedInstance[]>;
  /** 订阅服务变更（可选）：实例列表变化时回调，返回取消订阅函数 */
  subscribe?(serviceName: string, listener: (instances: ResolvedInstance[]) => void): Promise<() => void>;
}
