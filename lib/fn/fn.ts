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
import {BaseColumn} from '../schema/base_column';
import {AggregatedColumn} from './aggregated_column';
import {StarColumn} from './star_column';

// Keep lower case class name for compatibility with Lovefield API.
// tslint:disable:class-name
// TODO(arthurhsu): FIXME: use Column instead of BaseColumn.
// @export
export class fn {
  public static avg(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.AVG);
  }

  public static count(column?: BaseColumn): BaseColumn {
    const col: BaseColumn = column || new StarColumn();
    return new AggregatedColumn(col, FnType.COUNT);
  }

  public static distinct(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.DISTINCT);
  }

  public static max(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.MAX);
  }

  public static min(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.MIN);
  }

  public static stddev(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.STDDEV);
  }

  public static sum(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.SUM);
  }

  public static geomean(col: BaseColumn): BaseColumn {
    return new AggregatedColumn(col, FnType.GEOMEAN);
  }
}
// tslint:enable:class-name
