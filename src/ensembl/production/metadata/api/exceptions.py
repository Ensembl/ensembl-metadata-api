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


class MetaException(Exception):
    """Base Metadata API Exception class"""
    pass


class UpdaterException(MetaException, RuntimeError):
    """An error occurred while updating"""
    pass


class MetadataUpdateException(MetaException, RuntimeError):
    """An error occurred while updating metadata"""
    pass


class WrongReleaseException(MetaException, RuntimeError):
    """Wrong release in core meta"""
    pass


class TaxonNotFoundException(MetaException, RuntimeError):
    """Taxon not found in taxonomy"""
    pass


class MissingMetaException(MetaException, RuntimeError):
    """Missing Mandatory key in core meta"""
    pass


class UpdateBackCoreException(UpdaterException, RuntimeError):
    """An error occurred while updating back the core database"""
    pass


class TypeNotFoundException(UpdaterException, RuntimeError):
    """Dataset Type not found"""
    pass


class DatasetFactoryException(Exception):
    """An error occured while using dataset factory"""
    pass


class ExistingGenomeIdCoreException(UpdaterException):
    """ Meta table in core defines a genome_uuid key but it doesn't match with the one already in metadata db"""
    pass
