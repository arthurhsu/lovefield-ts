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

import {FnType} from '../base/private_enum';
import {AggregatedColumn} from './aggregated_column';
import {Column} from '../schema/column';
import {StarColumn} from './star_column';

// Keep lower case class name for compatibility with Lovefield API.
// @export
export class fn {
  static avg(col: Column): Column {
    return new AggregatedColumn(col, FnType.AVG);
  }

  static count(column?: Column): Column {
    const col: Column = column || new StarColumn();
    return new AggregatedColumn(col, FnType.COUNT);
  }

  static distinct(col: Column): Column {
    return new AggregatedColumn(col, FnType.DISTINCT);
  }

  static max(col: Column): Column {
    return new AggregatedColumn(col, FnType.MAX);
  }

  static min(col: Column): Column {
    return new AggregatedColumn(col, FnType.MIN);
  }

  static stddev(col: Column): Column {
    return new AggregatedColumn(col, FnType.STDDEV);
  }

  static sum(col: Column): Column {
    return new AggregatedColumn(col, FnType.SUM);
  }

  static geomean(col: Column): Column {
    return new AggregatedColumn(col, FnType.GEOMEAN);
  }
}
