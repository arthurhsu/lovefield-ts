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
import { MapSet } from '../structs/map_set';

// The following states represent the life cycle of a transaction. These states
// are exclusive meaning that a tx can be only on one state at a given time.
export enum TransactionState {
  CREATED = 0,
  ACQUIRING_SCOPE = 1,
  ACQUIRED_SCOPE = 2,
  EXECUTING_QUERY = 3,
  EXECUTING_AND_COMMITTING = 4,
  COMMITTING = 5,
  ROLLING_BACK = 6,
  FINALIZED = 7,
}

export class StateTransition {
  static get(): StateTransition {
    if (!StateTransition.instance) {
      StateTransition.instance = new StateTransition();
    }
    return StateTransition.instance;
  }

  private static instance: StateTransition;
  private map: MapSet<TransactionState, TransactionState>;
  constructor() {
    this.map = new MapSet<TransactionState, TransactionState>();
    const TS = TransactionState;
    this.map.set(TS.CREATED, TS.ACQUIRING_SCOPE);
    this.map.set(TS.CREATED, TS.EXECUTING_AND_COMMITTING);
    this.map.set(TS.ACQUIRING_SCOPE, TS.ACQUIRED_SCOPE);
    this.map.set(TS.ACQUIRED_SCOPE, TS.EXECUTING_QUERY);
    this.map.set(TS.ACQUIRED_SCOPE, TS.COMMITTING);
    this.map.set(TS.ACQUIRED_SCOPE, TS.ROLLING_BACK);
    this.map.set(TS.EXECUTING_QUERY, TS.ACQUIRED_SCOPE);
    this.map.set(TS.EXECUTING_QUERY, TS.FINALIZED);
    this.map.set(TS.EXECUTING_AND_COMMITTING, TS.FINALIZED);
    this.map.set(TS.COMMITTING, TS.FINALIZED);
    this.map.set(TS.ROLLING_BACK, TS.FINALIZED);
  }

  get(current: TransactionState): Set<TransactionState> {
    return this.map.getSet(current);
  }
}
