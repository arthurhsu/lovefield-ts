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

import {Type} from '../base/enum';
import {Index} from './index';
import {Table} from './table';

export interface Column {
  getName(): string;
  getNormalizedName(): string;
  getTable(): Table;
  getType(): Type;
  getAlias(): string;
  getIndices(): Index[];
  // The index that refers only to this column, or null if such index does
  // not exist.
  getIndex(): Index;
  isNullable(): boolean;
}
