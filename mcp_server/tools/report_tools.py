from typing import Dict, Any
import json
import csv
from io import StringIO


class ReportTools:
    def __init__(self, mongo_uri: str = None, database_name: str = None):
        # We'll use the existing database configuration instead of these params
        pass

    async def generate_report(self, report_config: dict) -> Dict[str, Any]:
        """Generate a report based on the configuration"""
        report_type = report_config.get("type", "json")
        data = report_config.get("data", [])
        title = report_config.get("title", "MongoDB Query Report")
        
        if report_type == "json":
            return {
                "title": title,
                "format": "json",
                "data": data,
                "record_count": len(data) if isinstance(data, list) else 1
            }
        elif report_type == "csv":
            return await self.format_data({"data": data}, "csv")
        else:
            return {
                "error": f"Unsupported report type: {report_type}",
                "supported_types": ["json", "csv"]
            }

    async def format_data(self, data: dict, format_type: str) -> Dict[str, Any]:
        """Format data in the specified format"""
        raw_data = data.get("data", [])
        
        if format_type == "json":
            return {
                "format": "json",
                "data": raw_data,
                "formatted_data": json.dumps(raw_data, indent=2, default=str)
            }
        elif format_type == "csv":
            if not raw_data or not isinstance(raw_data, list):
                return {
                    "format": "csv",
                    "error": "No data available or data is not in list format"
                }
            
            # Convert MongoDB documents to CSV
            output = StringIO()
            if raw_data:
                # Get all unique keys from all documents
                all_keys = set()
                for doc in raw_data:
                    if isinstance(doc, dict):
                        all_keys.update(doc.keys())
                
                fieldnames = sorted(list(all_keys))
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                
                for doc in raw_data:
                    if isinstance(doc, dict):
                        # Convert ObjectId and other non-serializable types to strings
                        row = {k: str(v) if v is not None else "" for k, v in doc.items()}
                        writer.writerow(row)
            
            return {
                "format": "csv",
                "data": raw_data,
                "formatted_data": output.getvalue(),
                "record_count": len(raw_data)
            }
        else:
            return {
                "error": f"Unsupported format: {format_type}",
                "supported_formats": ["json", "csv"]
            }
