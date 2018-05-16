/**
 * Copyright 2018 The Lovefield Project Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ErrorCode, Exception} from './exception';
import {ServiceId} from './service_id';

export class Global {
  public static get() {
    if (!Global.instance) {
      Global.instance = new Global();
    }
    return Global.instance;
  }
  private static instance: Global;

  private services: Map<string, object>;

  constructor() {
    this.services = new Map<string, object>();
  }

  public clear(): void {
    this.services.clear();
  }

  public registerService<T>(serviceId: ServiceId<T>, service: T): T {
    this.services.set(serviceId.toString(), service as any as object);
    return service;
  }

  public getService<T>(serviceId: ServiceId<T>): T {
    const service = this.services.get(serviceId.toString());
    if (!service) {
      // 7: Service {0} not registered.
      throw new Exception(ErrorCode.SERVICE_NOT_FOUND, serviceId.toString());
    }
    return service as any as T;
  }

  public isRegistered<T>(serviceId: ServiceId<T>): boolean {
    return this.services.has(serviceId.toString());
  }

  public listServices(): IterableIterator<string> {
    return this.services.keys();
  }
}
