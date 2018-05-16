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

import {ConstraintAction, ConstraintTiming} from '../base/enum';
import {ErrorCode, Exception} from '../base/exception';

export interface RawForeignKeySpec {
  local: string;
  ref: string;
  action: ConstraintAction;
  timing: ConstraintTiming;
}

export class ForeignKeySpec {
  public childColumn: string;
  public parentTable: string;
  public parentColumn: string;
  // Normalized name of this foreign key constraint.
  public name: string;
  public action: ConstraintAction;
  public timing: ConstraintTiming;

  constructor(
      rawSpec: RawForeignKeySpec, readonly childTable: string, name: string) {
    const array = rawSpec.ref.split('.');
    if (array.length !== 2) {
      // 540: Foreign key {0} has invalid reference syntax.
      throw new Exception(ErrorCode.INVALID_FK_REF, name);
    }

    this.childColumn = rawSpec.local;
    this.parentTable = array[0];
    this.parentColumn = array[1];
    this.name = `${childTable}.${name}`;
    this.action = rawSpec.action;
    this.timing = rawSpec.timing;
  }
}
