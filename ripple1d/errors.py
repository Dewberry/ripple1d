class ProjectionNotFoundError(Exception):  # noqa: D100
    """TODO."""


class NoDefaultEPSGError(Exception):
    """TODO."""


class ModelNotFoundError(Exception):
    """TODO."""


class NotGeoreferencedError(Exception):
    """TODO."""


class CouldNotIdentifyPrimaryPlanError(Exception):
    """TODO."""


class NoFlowFileSpecifiedError(Exception):
    """TODO."""


class InvalidStructureDataError(Exception):
    """TODO."""


class NoGeometryFileSpecifiedError(Exception):
    """TODO."""


class NotAPrjFile(Exception):
    """TODO."""


class NoCRSInferredError(Exception):
    """TODO."""


class UnkownCRSUnitsError(Exception):
    """TODO."""


class HECRASVersionNotInstalledError(Exception):
    """TODO."""


class NoRiverLayerError(Exception):
    """TODO."""


class NoCrossSectionLayerError(Exception):
    """TODO."""


class FlowTitleAlreadyExistsError(Exception):
    """TODO."""


class PlanTitleAlreadyExistsError(Exception):
    """TODO."""


class CouldNotFindAnyPlansError(Exception):
    """TODO."""


class ToManyPlansError(Exception):
    """TODO."""


class RASComputeTimeoutError(Exception):
    """Raised on timeout of API call to Compute_CurrentPlan."""


class RASComputeError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates error."""


class RASComputeMeshError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates mesh-specific error."""


class RASGeometryError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates geometry-specific error."""


class RASStoreAllMapsError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates StoreAllMaps error (related to RAS Mapper postprocessing)."""


class DepthGridNotFoundError(Exception):
    """Raised when a depth grid is not found when clipping raw RAS output."""


class PlanNameNotFoundError(Exception):
    """Raised when a plan is not found during post processing depth grids and rating curve dbs."""


class UnknownVerticalUnits(Exception):
    """Raised when unknown vertical units are specified."""


class NullTerrainError(Exception):
    """Raised when the downloaded terrain for an error is all nodata values."""


class BadConflation(Exception):
    """Raised when conflation yields a d/s cross-section with higher station than the u/s cross-section."""


class SingleXSModel(Exception):
    """Raised when geopackage creation would yield a single cross-section model."""

