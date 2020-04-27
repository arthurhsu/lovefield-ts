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

import {ErrorCode} from './enum';
import {Exception} from './exception';
import {LovefieldOptions} from './lovefield_options';
import {DefaultOptions} from './options';
import {ServiceId} from './service_id';

export class Global {
  static get(): Global {
    if (!Global.instance) {
      Global.instance = new Global();
    }
    if (!Global.instance.opt) {
      Global.instance.setOptions(new DefaultOptions());
    }
    return Global.instance;
  }
  private static instance: Global;

  private services: Map<string, object>;
  private opt: LovefieldOptions;

  constructor() {
    this.services = new Map<string, object>();
  }

  clear(): void {
    this.services.clear();
  }

  registerService<T>(serviceId: ServiceId<T>, service: T): T {
    this.services.set(serviceId.toString(), (service as unknown) as object);
    return service;
  }

  getService<T>(serviceId: ServiceId<T>): T {
    const service = this.services.get(serviceId.toString());
    if (!service) {
      // 7: Service {0} not registered.
      throw new Exception(ErrorCode.SERVICE_NOT_FOUND, serviceId.toString());
    }
    return (service as unknown) as T;
  }

  isRegistered<T>(serviceId: ServiceId<T>): boolean {
    return this.services.has(serviceId.toString());
  }

  listServices(): string[] {
    return Array.from(this.services.keys());
  }

  getOptions(): LovefieldOptions {
    return this.opt;
  }

  setOptions(options: LovefieldOptions) {
    this.opt = options;
  }
}
