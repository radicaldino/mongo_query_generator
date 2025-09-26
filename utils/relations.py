from typing import Optional, Dict, Any, List
from config.database import get_db


def get_schema_relation(db: str) -> Dict[str, Any]:
        """Get predefined schema relations for a database"""
        # This is the static relations data - you might want to move this to a separate config file
        relation = {
            "Inventory": {
        "materialLossTransaction": {
            "connected_schemas": [
                "companies",
                "geodata",
                "material"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID",
                "geoDataID": "geodata.geo_data_id",
                "materialID": "material.material_id"
            },
            "description": "Use-case: Track and record material loss events. Goal: Log material losses due to damage, expiration, or other reasons for inventory reconciliation and reporting."
        },
        "attribute": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "companyId": "companies.companyID"
            },
            "description": "Use-case: Define and manage attributes for materials. Goal: Provide a flexible way to add descriptive properties to materials, such as color, size, or quality."
        },
        "barcode": {
            "connected_schemas": [
                "material",
                "companies"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Associate materials with unique barcodes for easy identification. Goal: Enable efficient scanning and tracking of materials in inventory operations."
        },
        "batchTransaction": {
            "connected_schemas": [
                "companies",
                "geodata",
                "materialType",
                "material"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID",
                "geoID": "geodata.geo_data_id",
                "material_type_id": "materialType.typeID",
                "materialID": "material.material_id"
            },
            "description": "Use-case: Record transactions related to specific material batches. Goal: Maintain a detailed history of material batches, including quantity changes, prices, and expiration dates."
        },
        "cogsCalculations": {
            "connected_schemas": [
                "companies",
                "material",
                "geodata"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID",
                "materialID": "material.material_id",
                "geoDataID": "geodata.geo_data_id"
            },
            "description": "Use-case: Calculate the cost of goods sold. Goal: Determine the total cost associated with producing and selling materials, including shipping, wastage, and production charges."
        },
        "dcitemtransactions": {
            "connected_schemas": [
                "deliveryChallans",
                "material"
            ],
            "foreign_keys": {
                "dcId": "deliveryChallans.dcId",
                "materialId": "material.material_id"
            },
            "description": "Use-case: Track individual items within a delivery challan. Goal: Record specific material quantities, batch codes, and status for items in transit."
        },
        "deliveryChallans": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID"
            },
            "description": "Use-case: Manage and track material transfers between locations. Goal: Create a record for goods being shipped from one warehouse to another, including status and file attachments."
        },
        "eComProductMapping": {
            "connected_schemas": [
                "geodata",
                "material",
                "companies"
            ],
            "foreign_keys": {
                "amazon_sp_geo_data_id": "geodata.geo_data_id",
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "uni_commerce_geo_data_id": "geodata.geo_data_id",
                "wix_geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Map internal materials to external e-commerce products. Goal: Synchronize product information and inventory across various e-commerce platforms like Amazon, Flipkart, and Wix."
        },
        "geodata": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Store and manage geographical location data. Goal: Provide a centralized schema for all location-related information, such as addresses, cities, and zip codes, for various company operations."
        },
        "integration": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID"
            },
            "description": "Use-case: Store API details for external system integrations. Goal: Maintain access tokens, credentials, and webhook information for connecting to e-commerce platforms and other third-party services."
        },
        "itemMaterialMapping": {
            "connected_schemas": [
                "material",
                "unitOfMeasurement",
                "companies"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "uom_id": "unitOfMeasurement.uom_id",
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Map materials to their specific units of measurement. Goal: Define the standard unit for a given material, which is critical for inventory management and transactions."
        },
        "material": {
            "connected_schemas": [
                "companies",
                "materialCategory",
                "uom",
                "materialType"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID",
                "material_category_id": "materialCategory.material_category_id",
                "uom_id": "uom.uom_id",
                "material_type_id": "materialType.material_type_id"
            },
            "description": "Use-case: Define and manage all materials in the system. Goal: Serve as the central repository for material data, including names, descriptions, and related properties."
        },
        "materialCategory": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Categorize materials for better organization. Goal: Group similar materials together for easier inventory management and reporting."
        },
        "materialInventory": {
            "connected_schemas": [
                "material",
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Track the quantity of materials in inventory. Goal: Maintain a real-time record of available, possible, and bad inventory quantities for each material at a specific location."
        },
        "materialPrice": {
            "connected_schemas": [
                "material",
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Record the pricing information for materials. Goal: Store historical and current unit prices and buying prices for each material, often tied to a specific batch and location."
        },
        "materialTransaction": {
            "connected_schemas": [
                "material",
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Log all material movements and changes. Goal: Maintain a detailed history of every transaction affecting material quantity or status, including receipts, issues, and adjustments."
        },
        "materialTransfer": {
            "connected_schemas": [
                "material",
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "material_source_geo_data_id": "geodata.geo_data_id",
                "material_destination_geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Document material transfers between locations. Goal: Track the movement of materials from a source location to a destination location, recording quantities and dates."
        },
        "materialType": {
            "connected_schemas": [],
            "foreign_keys": {},
            "description": "Use-case: Classify materials by their type. Goal: Define different categories of materials, such as raw, finished, or packaging materials, for organizational purposes."
        },
        "metaData": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Define and manage metadata for materials. Goal: Store general metadata values and options that can be applied to various materials, such as color, size, or quality."
        },
        "metaDataMaterialMapping": {
            "connected_schemas": [
                "companies",
                "material",
                "metaData"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID",
                "material_id": "material.material_id",
                "selected_metaData_id": "metaData.metaData_id"
            },
            "description": "Use-case: Map specific metadata options to materials. Goal: Associate predefined metadata values with specific materials for detailed tracking and filtering."
        },
        "metaDataOptions": {
            "connected_schemas": [
                "companies",
                "metaData"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID",
                "metaData_id": "metaData.metaData_id"
            },
            "description": "Use-case: Define options for various metadata. Goal: Create a set of selectable values for each metadata type, such as 'red', 'green', and 'blue' for the 'color' metadata."
        },
        "possibleStock": {
            "connected_schemas": [
                "material",
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "company_id": "companies.companyID",
                "location_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Track materials that are potentially available in the warehouse. Goal: Maintain a record of materials that are in transit or pending receipt to provide a forward-looking view of inventory."
        },
        "possibleStockTransaction": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Record stock transactions that are pending or in progress. Goal: Maintain a history of potential stock movements before they are finalized, often for reconciliation and auditing."
        },
        "profitAndLoss": {
            "connected_schemas": [
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID",
                "geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Calculate and store company-wide profit and loss. Goal: Provide a summary of sales, revenue, and margin over specific periods to support financial reporting."
        },
        "profitAndLossTransactions": {
            "connected_schemas": [
                "companies",
                "material",
                "geodata"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID",
                "material_id": "material.material_id",
                "geo_data_id": "geodata.geo_data_id"
            },
            "description": "Use-case: Log individual transactions that contribute to profit and loss. Goal: Maintain a detailed record of sales, purchases, and costs for each material to provide granular insights for financial analysis."
        },
        "reports": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Store information related to generated reports. Goal: Maintain a record of all reports, including their type, file path, and timestamps, for easy access and tracking."
        },
        "skuItemMapping": {
            "connected_schemas": [
                "companies",
                "geodata"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID",
                "geoDataID": "geodata.geo_data_id"
            },
            "description": "Use-case: Map SKUs to specific items and their properties. Goal: Associate each SKU with an item name, source, and stock quantity, providing a centralized lookup for product information."
        },
        "uom": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Define units of measurement. Goal: Provide a standardized list of units (e.g., kg, pieces, liters) that can be associated with materials and products."
        },
        "userDataMapping": {
            "connected_schemas": [
                "companies"
            ],
            "foreign_keys": {
                "companyID": "companies.companyID"
            },
            "description": "Use-case: Map user data to their respective companies and inventory settings. Goal: Associate users with their company-specific configurations and permissions, streamlining access to inventory data."
        },
        "warehouseMaterialTransactions": {
            "connected_schemas": [
                "material",
                "geodata",
                "companies"
            ],
            "foreign_keys": {
                "material_id": "material.material_id",
                "geo_data_id": "geodata.geo_data_id",
                "company_id": "companies.companyID"
            },
            "description": "Use-case: Record all material transactions within a warehouse. Goal: Maintain a detailed log of material movements, including quantity changes, aisle, rack, and bin locations."
        }
    }
}
        # Get relations for the specific database
        db_relations = relation.get(db, {})
        
        if not db_relations:
            return {
                "success": False,
                "error": f"No schema relations defined for database '{db}'. Supported databases: {list(relation.keys())}",
                "database": db,
                "relations": {},
                "total_schemas": 0
            }
        
        return {
            "success": True,
            "database": db,
            "relations": db_relations,
            "total_schemas": len(db_relations)
        }
