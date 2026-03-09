# MS Fabric Deployment Checklist

## Quick Reference - Print This!

**Estimated Time:** 45-60 minutes  
**Date Started:** _______________

---

## ☐ PHASE 1: Local Preparation (15 min)

### Step 1: Verify Environment
```bash
cd fabric_data_quality
conda activate fabric-dq
python -c "from dq_framework import DataQualityValidator; print('✅ Ready')"
```
- [ ] Environment activated
- [ ] Framework verified

### Step 2: Profile Your Data
```bash
python profile_data.py your_data.csv --output config/validation.yml
```
- [ ] Data profiled
- [ ] Config generated
- [ ] Quality score noted: _______%

### Step 3: Create Layer Configs
```bash
cp config/validation.yml config/bronze_validation.yml
cp config/validation.yml config/silver_validation.yml
cp config/validation.yml config/gold_validation.yml
```
- [ ] Bronze config created (lenient)
- [ ] Silver config created (strict)
- [ ] Gold config created (very strict)
- [ ] Business rules added

### Step 4: Prepare Deployment Package
```bash
mkdir fabric_deployment_package
cp config/*_validation.yml fabric_deployment_package/
```
- [ ] Deployment folder created
- [ ] Configs copied

---

## ☐ PHASE 2: Fabric Workspace Setup (10 min)

### Step 1: Access Fabric
- [ ] Logged into https://app.fabric.microsoft.com
- [ ] Workspace selected: ___________________

### Step 2: Create Lakehouse
- [ ] Lakehouse created
- [ ] Lakehouse name: ___________________

### Step 3: Verify Structure
- [ ] Tables section visible
- [ ] Files section visible

---

## ☐ PHASE 3: Upload Files (15 min)

### Step 1: Create Folders
In Lakehouse Files section:
- [ ] Created: `dq_configs/`
- [ ] Created: `dq_logs/`
- [ ] Created: `raw_data/`

### Step 2: Upload Configs
- [ ] Uploaded: bronze_validation.yml
- [ ] Uploaded: silver_validation.yml
- [ ] Uploaded: gold_validation.yml
- [ ] Uploaded: (other configs): ___________________

### Step 3: Upload Data (Optional)
- [ ] Sample data uploaded
- [ ] Data path noted: ___________________

### Step 4: Verify
- [ ] Files visible in Files/dq_configs/
- [ ] Can preview YAML files

---

## ☐ PHASE 4: Create DQ Module (10 min)

### Step 1: Create Notebook
- [ ] New notebook created
- [ ] Named: `DQ_Module`

### Step 2: Install Dependencies (Cell 1)
```python
%pip install pyyaml great-expectations>=1.0.0,<2.0.0 pandas pyarrow --quiet
```
- [ ] Dependencies installed (no errors)

### Step 3: Add Imports (Cell 2)
```python
import pandas as pd
from great_expectations.data_context import EphemeralDataContext
# ... (other imports)
```
- [ ] Imports successful

### Step 4: Add Validator Class (Cell 3)
- [ ] FabricDataQualityValidator class pasted
- [ ] Cell ran successfully

### Step 5: Test Module (Cell 4)
```python
validator = FabricDataQualityValidator('Files/dq_configs/bronze_validation.yml')
```
- [ ] Config loaded successfully
- [ ] Expectations count: _______

### Step 6: Save Notebook
- [ ] Notebook saved

---

## ☐ PHASE 5: Test & Validate (10 min)

### Step 1: Create Test Notebook
- [ ] New notebook created
- [ ] Named: `Test_DQ_Integration`

### Step 2: Import DQ Module (Cell 1)
```python
%run DQ_Module
```
- [ ] Module imported successfully

### Step 3: Load Data (Cell 2)
- [ ] Data loaded
- [ ] Row count: __________
- [ ] Column count: __________

### Step 4: Run Bronze Validation (Cell 3)
- [ ] Bronze validation ran
- [ ] Quality score: _______%
- [ ] Passed checks: _______
- [ ] Failed checks: _______

### Step 5: Run Silver Validation (Cell 4)
- [ ] Data transformed
- [ ] Silver validation ran
- [ ] Quality score: _______%

### Step 6: Save to Tables (Cell 5)
- [ ] Bronze table created
- [ ] Silver table created (if quality met threshold)

### Step 7: Verify Tables (Cell 6)
- [ ] Tables visible in Lakehouse
- [ ] Can query tables

### Step 8: Check Logs (Cell 7)
- [ ] Log files created in dq_logs/
- [ ] Can read log contents

---

## ☐ POST-DEPLOYMENT

### Documentation
- [ ] Team notified of deployment
- [ ] Usage guide shared
- [ ] Training session scheduled

### Monitoring Setup
- [ ] Monitoring table created: `dq_pipeline_monitoring`
- [ ] Power BI dashboard planned

### Next Steps
- [ ] Real data test scheduled
- [ ] Production pipeline creation planned
- [ ] Additional datasets identified for profiling

---

## 📊 Deployment Results

**Deployment Date:** _______________  
**Deployment Time:** _______ minutes  
**Deployed By:** _______________  
**Workspace:** _______________  
**Lakehouse:** _______________  

**Test Results:**
- Bronze Quality: _______%
- Silver Quality: _______%
- Tables Created: _______
- Logs Generated: _______

**Status:** ☐ Success  ☐ Partial  ☐ Failed

**Notes:**
_____________________________________________
_____________________________________________
_____________________________________________

---

## 🆘 Common Issues & Quick Fixes

| Issue | Quick Fix |
|-------|-----------|
| Config not found | Check path: `Files/dq_configs/file.yml` (no leading /) |
| Module not found | Run: `%pip install great-expectations>=1.0.0,<2.0.0` |
| Memory error | Sample data: `df.sample(n=100000)` |
| Table not found | Use: `spark.createDataFrame(df).write.mode("overwrite").saveAsTable("name")` |

---

## 📞 Support Contacts

**Data Engineering Team:** _______________  
**Fabric Admin:** _______________  
**Framework Documentation:** `FABRIC_DEPLOYMENT_GUIDE.md`

---

**Framework Version:** 2.0.0
**Last Updated:** 2026-01-19
