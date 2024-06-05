# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata

__all__ = ['LoadAble']


class LoadAble(object):
    def __repr__(self):
        from ensembl.production.metadata.api.models import DatasetStatus, ReleaseStatus
        class_name = self.__class__.__name__
        attributes = {name: value for name, value in self.__dict__.items() if
                      isinstance(value, (type(None), str, int, float, bool, DatasetStatus, ReleaseStatus))}

        return '<{}({})>'.format(class_name, attributes)
