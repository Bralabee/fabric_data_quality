"""
Usage Examples for Fabric Data Quality Framework
This file demonstrates how to use the framework across different projects
"""

# =============================================================================
# Example 1: Basic Usage with Spark DataFrame (HSS Project)
# =============================================================================

def example_1_hss_basic_validation():
    """
    Example: Validate HSS incidents data in a Fabric notebook
    Project: full_stack_hss
    """
    from dq_framework import FabricDataQualityRunner
    
    # Load your data
    df = spark.read.table("silver.hss_incidents")
    
    # Run validation
    runner = FabricDataQualityRunner(
        config_path="/Workspace/fabric_data_quality/examples/hss_incidents_example.yml"
    )
    
    results = runner.validate_spark_dataframe(df)
    
    # Check results
    if results["success"]:
        print(f"✅ Validation passed! {results['statistics']['successful_expectations']} checks passed")
    else:
        print(f"❌ Validation failed! {results['statistics']['unsuccessful_expectations']} checks failed")
        
        # Print failed expectations
        for result in results["results"]:
            if not result["success"]:
                print(f"  - {result['expectation_type']}: {result.get('exception_message', 'Failed')}")
    
    return results


# =============================================================================
# Example 2: Delta Table Validation with Error Handling (AIMS Project)
# =============================================================================

def example_2_aims_delta_validation():
    """
    Example: Validate AIMS data from Delta table with custom error handling
    Project: AIMS_LOCAL
    """
    from dq_framework import FabricDataQualityRunner
    
    # Initialize runner with custom failure handling
    runner = FabricDataQualityRunner(
        config_path="/Workspace/fabric_data_quality/examples/aims_data_example.yml"
    )
    
    try:
        # Validate Delta table directly
        results = runner.validate_delta_table(
            table_name="aims.data_platform"
        )
        
        # Generate quality report
        print(f"""
        Data Quality Report
        ===================
        Total Expectations: {results['statistics']['evaluated_expectations']}
        Passed: {results['statistics']['successful_expectations']}
        Failed: {results['statistics']['unsuccessful_expectations']}
        Success Rate: {results['statistics']['success_percent']:.2f}%
        """)
        
        # Handle failures based on severity
        critical_failures = [
            r for r in results["results"] 
            if not r["success"] and r.get("meta", {}).get("severity") == "critical"
        ]
        
        if critical_failures:
            print("⚠️ Critical failures detected:")
            for failure in critical_failures:
                print(f"  - {failure['expectation_type']}")
            raise ValueError("Critical data quality issues found!")
            
    except Exception as e:
        print(f"Validation failed: {e}")
        # Log to Fabric monitoring
        # mssparkutils.notebook.exit(str(e))
        raise
    
    return results


# =============================================================================
# Example 3: Lakehouse File Validation (ACA Project)
# =============================================================================

def example_3_aca_lakehouse_validation():
    """
    Example: Validate SharePoint migration files in Lakehouse
    Project: ACA_COMMERCIAL
    """
    from dq_framework import FabricDataQualityRunner
    
    runner = FabricDataQualityRunner(
        config_path="/Workspace/fabric_data_quality/examples/aca_commercial_example.yml"
    )
    
    # Validate file from Lakehouse
    results = runner.validate_lakehouse_file(
        lakehouse_name="ACA_Lakehouse",
        file_path="Files/commercial/sharepoint_files.parquet",
        file_format="parquet"
    )
    
    # Save validation results for audit
    results_df = spark.createDataFrame([{
        "validation_date": datetime.now(),
        "validation_name": results["validation_name"],
        "success": results["success"],
        "total_checks": results["statistics"]["evaluated_expectations"],
        "passed_checks": results["statistics"]["successful_expectations"],
        "failed_checks": results["statistics"]["unsuccessful_expectations"]
    }])
    
    results_df.write.mode("append").saveAsTable("aca.dq_validation_history")
    
    return results


# =============================================================================
# Example 4: Multiple Config Files (Multi-Layer Validation)
# =============================================================================

def example_4_multi_layer_validation():
    """
    Example: Validate data through bronze, silver, gold layers
    """
    from dq_framework import ConfigLoader, DataQualityValidator
    import pandas as pd
    
    # Load and merge multiple configs
    loader = ConfigLoader()
    bronze_config = loader.load("/Workspace/fabric_data_quality/config_templates/bronze_layer_template.yml")
    silver_config = loader.load("/Workspace/fabric_data_quality/config_templates/silver_layer_template.yml")
    
    # Validate bronze layer
    bronze_df = spark.read.table("bronze.raw_data")
    bronze_validator = DataQualityValidator(bronze_config)
    bronze_results = bronze_validator.validate(bronze_df.toPandas())
    
    if not bronze_results["success"]:
        raise ValueError("Bronze layer validation failed!")
    
    # Validate silver layer
    silver_df = spark.read.table("silver.cleaned_data")
    silver_validator = DataQualityValidator(silver_config)
    silver_results = silver_validator.validate(silver_df.toPandas())
    
    return {
        "bronze": bronze_results,
        "silver": silver_results
    }


# =============================================================================
# Example 5: Custom Config with Quick Validate
# =============================================================================

def example_5_quick_validate():
    """
    Example: Quick validation without config file (inline expectations)
    """
    from dq_framework.fabric_connector import quick_validate
    
    # Your data
    df = spark.read.table("my_table")
    
    # Quick validation with inline config
    config = {
        "validation_name": "quick_check",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "id"}
            }
        ]
    }
    
    results = quick_validate(df, config)
    
    if not results["success"]:
        print("⚠️ Quick validation failed!")
    
    return results


# =============================================================================
# Example 6: Integration with Fabric Pipeline
# =============================================================================

def example_6_fabric_pipeline_integration():
    """
    Example: Use in Fabric notebook as part of ETL pipeline
    """
    from dq_framework import FabricDataQualityRunner
    from notebookutils import mssparkutils
    
    # Configuration
    CONFIG_PATH = "/Workspace/fabric_data_quality/examples/hss_incidents_example.yml"
    
    # Initialize runner
    runner = FabricDataQualityRunner(CONFIG_PATH)
    
    # ETL Step 1: Extract
    df = spark.read.table("bronze.source_data")
    
    # ETL Step 2: Transform
    transformed_df = df.filter("status = 'Active'").dropDuplicates(["id"])
    
    # ETL Step 3: Validate (before loading)
    validation_results = runner.validate_spark_dataframe(transformed_df)
    
    # ETL Step 4: Load (only if validation passes)
    if validation_results["success"]:
        transformed_df.write.mode("overwrite").saveAsTable("silver.validated_data")
        print("✅ Data validated and loaded successfully")
    else:
        # Handle failure
        error_msg = f"Validation failed: {validation_results['statistics']['unsuccessful_expectations']} checks failed"
        print(f"❌ {error_msg}")
        
        # Write failed data to quarantine table
        transformed_df.write.mode("overwrite").saveAsTable("silver.quarantine_data")
        
        # Exit notebook with error
        mssparkutils.notebook.exit(error_msg)
    
    return validation_results


# =============================================================================
# Example 7: Batch Validation Across Multiple Tables
# =============================================================================

def example_7_batch_validation():
    """
    Example: Validate multiple tables in a loop
    """
    from dq_framework import FabricDataQualityRunner
    
    # Define tables and their configs
    validation_tasks = [
        {
            "table": "silver.hss_incidents",
            "config": "/Workspace/fabric_data_quality/examples/hss_incidents_example.yml",
            "name": "HSS Incidents"
        },
        {
            "table": "aims.data_platform",
            "config": "/Workspace/fabric_data_quality/examples/aims_data_example.yml",
            "name": "AIMS Data"
        },
        {
            "table": "aca.sharepoint_files",
            "config": "/Workspace/fabric_data_quality/examples/aca_commercial_example.yml",
            "name": "ACA SharePoint"
        }
    ]
    
    results_summary = []
    
    for task in validation_tasks:
        print(f"Validating {task['name']}...")
        
        runner = FabricDataQualityRunner(task["config"])
        df = spark.read.table(task["table"])
        results = runner.validate_spark_dataframe(df)
        
        results_summary.append({
            "table": task["table"],
            "name": task["name"],
            "success": results["success"],
            "success_rate": results["statistics"]["success_percent"]
        })
        
        print(f"  Success: {results['success']} ({results['statistics']['success_percent']:.2f}%)")
    
    # Print summary
    print("\n" + "="*60)
    print("Validation Summary")
    print("="*60)
    for summary in results_summary:
        status = "✅" if summary["success"] else "❌"
        print(f"{status} {summary['name']}: {summary['success_rate']:.2f}%")
    
    return results_summary


# =============================================================================
# Example 8: Custom Failure Handler
# =============================================================================

def example_8_custom_failure_handler():
    """
    Example: Custom handling of validation failures
    """
    from dq_framework import FabricDataQualityRunner
    
    def custom_failure_handler(results):
        """Custom logic for handling failures"""
        if not results["success"]:
            # Group failures by severity
            severity_groups = {}
            for result in results["results"]:
                if not result["success"]:
                    severity = result.get("meta", {}).get("severity", "unknown")
                    if severity not in severity_groups:
                        severity_groups[severity] = []
                    severity_groups[severity].append(result)
            
            # Handle based on severity
            if "critical" in severity_groups:
                print(f"⛔ CRITICAL: {len(severity_groups['critical'])} critical failures - STOPPING PIPELINE")
                raise ValueError("Critical data quality failure")
            
            elif "high" in severity_groups:
                print(f"⚠️ WARNING: {len(severity_groups['high'])} high severity failures - ALERTING TEAM")
                # Send notification to team
                # send_team_alert(severity_groups['high'])
            
            else:
                print(f"ℹ️ INFO: Low/medium severity failures - LOGGING ONLY")
                # Log for monitoring
    
    # Use custom handler
    runner = FabricDataQualityRunner(
        config_path="/Workspace/fabric_data_quality/examples/hss_incidents_example.yml"
    )
    
    df = spark.read.table("silver.hss_incidents")
    results = runner.validate_spark_dataframe(df)
    
    custom_failure_handler(results)
    
    return results


# =============================================================================
# Main execution (for testing)
# =============================================================================

if __name__ == "__main__":
    print("Fabric Data Quality Framework - Usage Examples")
    print("=" * 60)
    print("\nThese examples demonstrate various usage patterns.")
    print("Copy and adapt them to your specific needs.\n")
    
    # Uncomment to run examples:
    # example_1_hss_basic_validation()
    # example_2_aims_delta_validation()
    # example_3_aca_lakehouse_validation()
    # example_4_multi_layer_validation()
    # example_5_quick_validate()
    # example_6_fabric_pipeline_integration()
    # example_7_batch_validation()
    # example_8_custom_failure_handler()
