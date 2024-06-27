class ProjectionNotFoundError(Exception):
    pass


class NoDefaultEPSGError(Exception):
    pass


class ModelNotFoundError(Exception):
    pass


class NotGeoreferencedError(Exception):
    pass


class CouldNotIdentifyPrimaryPlanError(Exception):
    pass


class NoFlowFileSpecifiedError(Exception):
    pass


class NoGeometryFileSpecifiedError(Exception):
    pass


class NotAPrjFile(Exception):
    pass


class NoCRSInferredError(Exception):
    pass


class UnkownCRSUnitsError(Exception):
    pass


class HECRASVersionNotInstalledError(Exception):
    pass


class NoRiverLayerError(Exception):
    pass


class NoCrossSectionLayerError(Exception):
    pass


class FlowTitleAlreadyExistsError(Exception):
    pass


class PlanTitleAlreadyExistsError(Exception):
    pass


class CouldNotFindAnyPlansError(Exception):
    pass


class ToManyPlansError(Exception):
    pass


class RASComputeTimeoutError(Exception):
    """Raised on timeout of API call to Compute_CurrentPlan."""


class RASComputeError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates error"""


class RASComputeMeshError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates mesh-specific error"""


class RASGeometryError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates geometry-specific error"""


class RASStoreAllMapsError(Exception):
    """Raised when *.pNN.computeMsgs.txt indicates StoreAllMaps error (related to RAS Mapper postprocessing)"""


class DepthGridNotFoundError(Exception):
    """Raised when a depth grid is not found when clipping raw RAS output"""
