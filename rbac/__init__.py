"""Role-based access control: DB-backed roles and permission catalog sync."""

from rbac.repository import RbacRepository
from rbac.seed import seed_rbac_defaults

__all__ = ["RbacRepository", "seed_rbac_defaults"]
