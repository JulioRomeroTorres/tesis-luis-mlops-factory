from typing import Dict, Any
from kedro.framework.hooks import hook_impl
from kedro.io import AbstractDataset, DataCatalog


class CatalogWrapper(AbstractDataset):
    def __init__(self, catalog):
        self._catalog = catalog

    def _load(self):
        return self._catalog

    def _describe(self) -> Dict[str, Any]:
        return {"name": "CatalogWrapper", "catalog_type": str(type(self._catalog))}

    def _save(self, data) -> None:
        raise NotImplementedError("No se puede guardar el catálogo como dataset")

class ExposeKedroCatalogHook:

    @hook_impl
    def after_catalog_created(self, catalog: DataCatalog) -> None:
        catalog.add("catalog", CatalogWrapper(catalog))