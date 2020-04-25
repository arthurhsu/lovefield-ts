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

import {BackStore} from '../backstore/back_store';
import {Cache} from '../cache/cache';
import {IndexStore} from '../index/index_store';
import {QueryEngine} from '../proc/query_engine';
import {Runner} from '../proc/runner';
import {DatabaseSchema} from '../schema/database_schema';
import {ObserverRegistry} from './observer_registry';
import {ServiceId} from './service_id';

export class Service {
  // The backing data store used by this connection.
  // following statement fail compilation, need solution.
  static BACK_STORE = new ServiceId<BackStore>('backstore');

  // The shared row cache used by this connection.
  static CACHE = new ServiceId<Cache>('cache');

  // The shared store of all indices defined.
  static INDEX_STORE = new ServiceId<IndexStore>('indexstore');

  // Query engine used for generating execution plan.
  static QUERY_ENGINE = new ServiceId<QueryEngine>('engine');

  // Query runner which executes transactions.
  static RUNNER = new ServiceId<Runner>('runner');

  // Observer registry storing all observing queries.
  static OBSERVER_REGISTRY = new ServiceId<ObserverRegistry>(
    'observerregistry'
  );

  // Finalized schema associated with this connection.
  static SCHEMA = new ServiceId<DatabaseSchema>('schema');
}
