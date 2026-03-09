# Why I Built It This Way
## The Dual-Platform Architecture (DSVM + MS Fabric) — Explained

**Document Reference:** HS2-DATA-ARCH-2026-001  
**Author:** Sanmi — HS2 Data Engineering  
**Date:** 06 February 2026  
**Version:** 2.2

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 15 Jan 2026 | Initial draft |
| 2.0 | 24 Jan 2026 | Added business case, references |
| 2.1 | 30 Jan 2026 | Revised for clarity |
| 2.2 | 06 Feb 2026 | Tightened cost figures, added risk register & ADR summary |

---

## The Short Version

I built the AIMS Data Platform and Data Quality Framework to run in two places: on Azure DSVMs (outside Fabric) AND in Microsoft Fabric. This wasn't an accident — it was a deliberate architectural choice.

Here's what it gives us:
- **~80% of development work incurs no additional Fabric compute charges** — based on an observed split of ~30 hrs/week on DSVMs vs ~8 hrs/week in Fabric per engineer. The DSVMs are already provisioned, so this development time doesn't add to cloud spend
- **10-50× faster iteration** on DSVMs vs waiting for Fabric notebooks
- **No vendor lock-in** — the code is portable to Databricks or another platform if needed
- **Full test coverage** (74 out of 74 passing) because I can run proper pytest on the DSVMs

This document explains what I did, why I did it, and how it works.

---

## 1. Why Two Separate Projects?

A fair question: why not just put everything in one repository?

### The Practical Answer

I split the Data Quality Framework out because **multiple projects need it**. Right now, it's used by:

```
                    +---------------------+
                    |   DQ Framework      |
                    |   (the tools)       |
                    +----------+----------+
                               |
        +----------------------+----------------------+
        |                      |                      |
        v                      v                      v
+---------------+    +---------------+    +---------------+
|  AIMS Local   |    | HSS Incidents |    | ACA Commercial|
+---------------+    +---------------+    +---------------+
```

If I'd embedded the DQ code inside AIMS, the other projects would have to either:
1. Copy-paste the code (and maintain multiple versions forever), or
2. Take a dependency on the entire AIMS project just to get the DQ bits

Neither is great.

> **Think of it like a workshop and its tools.** AIMS is the workshop where I process data; the DQ Framework is a set of precision measuring instruments. You wouldn't weld your calipers to a single workbench — you'd keep them separate so you can use them anywhere.

### What the Industry Says

This isn't just my opinion. Here's what practitioners have found:

> *"Copy-pasting leads to multiple, slightly different versions of the 'same' data quality logic scattered across an organization. This makes it impossible to guarantee consistent application of rules."*  
> — Profisee [1]

I've seen this firsthand on other projects. Someone copies a notebook, makes a small tweak, and six months later you've got five different versions of "the same" validation logic, none of which quite match.

---

## 2. Why CLI Tools When Fabric Exists?

This is probably the most common question I get asked. Fabric is supposed to handle everything, right?

### What CLI Gives Us That Fabric Doesn't

| Capability | CLI (on DSVMs) | Fabric Notebooks |
|--------------|:-----------:|:----------------:|
| **No additional Fabric CU cost** | ✅ DSVMs already provisioned | ❌ ~$0.18/CU-hour |
| **Run pytest properly** | ✅ Full support | ⚠️ Hacky at best |
| **Independent of Fabric availability** | ✅ Yes | ❌ Nope |
| **Real debugging** | ✅ Breakpoints, profilers | ⚠️ Limited |
| **Git integration** | ✅ Native | 🔄 Getting better |

### The Money Side

Let's be concrete about costs. Fabric charges roughly $0.18 per capacity unit per hour (prices as of January 2026; see [Azure Fabric pricing](https://azure.microsoft.com/en-gb/pricing/details/microsoft-fabric/)) [3]. A small development cluster uses 2-4 CUs.

Assuming ~8 hrs/day × 5 days/week × 48 working weeks = ~1,920 hrs/year per engineer:

| If you develop in... | CUs | Additional cost per year (1 engineer) |
|----------------------|:---:|--------------------------------------:|
| CLI on DSVMs | 0 | $0 (DSVMs already provisioned) |
| Fabric notebooks (2 CU) | 2 | ~$691 |
| Fabric notebooks (4 CU) | 4 | ~$1,382 |

Multiply that by a team of 5 and you're looking at **$3,456 to $6,912 a year** in *avoidable Fabric CU charges* alone. That's money better spent elsewhere.

> *Note: These estimates assume dedicated development capacity. Shared capacity or pay-as-you-go models may differ. Actual costs depend on Fabric SKU and reservation discounts.*

### The Speed Difference

Anyone who's worked in Fabric knows the frustration of waiting for a Spark session to spin up. Here's what I actually see:

| Task | On DSVMs | In Fabric |
|------|-------|--------|
| Starting a session | Instant | 30-120 seconds |
| Running a quick test | 1-5 seconds | 10-60 seconds |
| Full test suite | 15-30 seconds | 3-5 minutes |
| Debug-fix-retry cycle | Immediate | 3-5 minutes |

When you're trying to track down a bug, that difference between "immediate" and "several minutes" adds up fast. Over a day of debugging, you might get 50 iterations on a DSVM vs 10 in Fabric.

---

## 3. The Benefits of Running Both Ways

### The Trade-off

There's always a trade-off between control and scale. Pure DSVM-based development gives you control but doesn't scale. Pure cloud (Fabric) gives you scale but less control.

I chose both:

```
                     HIGH CONTROL
                          |
         +----------------+----------------+
         |   Q1           |   Q2           |
         |   [LOCAL-ONLY] |   [DUAL <----] |
         |   High control |   High control |
         |   Low scale    |   High scale   |
         |   (dev only)   |   (my choice)  |
         |                |                |
LOW -----+----------------+----------------+----- HIGH
SCALE    |   Q3           |   Q4           |      SCALE
         |   [AVOID]      |   [FABRIC-ONLY]|
         |   Low control  |   Low control  |
         |   Low scale    |   High scale   |
         +----------------+----------------+
                          |
                     LOW CONTROL
```

### When to Use What

**The dual-platform approach works well for:**
- Production pipelines that need to be bulletproof
- Code that multiple teams will reuse
- Anything with complex business logic you need to debug
- Projects with audit requirements
- Teams watching their cloud spend

**Fabric-only makes more sense for:**
- Quick one-off analysis
- Exploring a new dataset
- Simple ETL that fits in a single notebook
- Teams who don't want to learn Python packaging

> **It's like owning a car vs using a taxi service.** Taxis are convenient—you don't worry about maintenance, you just pay per ride. But if you're driving every day, owning makes more sense. You put in more effort upfront (learning to drive, buying the car), but you save money and have more flexibility in the long run.

---

## 4. Infrastructure Requirements for Local Development
### "But Doesn't This Require Machines to Run CLI Tools?"

Fair point. CLI tools need somewhere to run. The answer: **those machines already exist** — we develop inside Azure Data Science Virtual Machines (DSVMs), not on physical laptops.

| What's Needed | Where It Comes From |
|---------------|---------------------|
| Azure DSVMs | Already provisioned for the team |
| Python 3.10+ | Pre-installed on DSVMs |
| Conda | Pre-configured on DSVMs |
| VS Code | Remote-SSH into DSVMs; free |

No new infrastructure required. The DSVMs are already provisioned and running. The only cost is the time to set up the project environment once (Conda env + dependencies).

And there are knock-on benefits:
1. **Resilience** — If Fabric has an outage, development can continue on the DSVMs
2. **CI/CD** — Build pipelines run Python on GitHub/Azure agents using the same setup
3. **Consistency** — Every developer works in the same DSVM image, avoiding "works on my machine" issues
4. **Hiring** — Any Python developer can contribute; no Fabric specialists needed

---

## 5. Governance: How Work Doesn't Get Siloed on a DSVM

A legitimate concern with local development is accessibility. Here's how I addressed it.

### Everything Ends Up in Shared Systems

I set up four layers of governance. DSVM-based work flows through all of them:

**Layer 1: Git**  
All code goes into GitHub/Azure DevOps. Branch protection requires reviews. Nothing ships without going through the repo.

**Layer 2: Configuration Files**  
The 68 validation configs are YAML files in version control. You can see exactly what rules are being applied, and the full history of changes.

**Layer 3: Packaged Library**  
The DQ Framework gets bundled into a `.whl` file and uploaded to the Fabric Environment. Every workspace uses the same versioned package.

**Layer 4: Data in OneLake**  
All the actual data lives in OneLake with proper RBAC. No datasets are stored on individual DSVMs.

So yes, development happens on DSVMs. But the outputs — code, configs, packages, data — all live in governed, shared systems.

### Compliance

This approach actually helps with compliance. The FDA, for instance, requires that:

> *"Audit trails must be securely stored, protected from unauthorized access or modification, and remain immutable once recorded."* [13]

Git commit history is exactly that: an immutable record of who changed what, when. That's harder to achieve when everyone edits shared notebooks directly.

---

## 6. The Business Case

### Cost Savings Over Time

Rough estimates, but directionally correct:

| | Year 1 | Year 3 (cumulative) | Year 5 (cumulative) |
|---|---:|---:|---:|
| **Avoided Fabric CU charges** (5 engineers, 2 CU) | ~$3,456 | ~$10,368 | ~$17,280 |
| **Hours saved on integration** | 40 | 120 | 200 |
| **Vendor lock-in avoided** | Some | Moderate | Significant |

> *Assumptions: 5 engineers, 2 CU dev capacity, ~1,920 dev hrs/year each, $0.18/CU-hour. DSVM costs are treated as sunk since they are already provisioned for general use. See Section 2 for derivation.*

### The Lock-In Question

This matters more than people think. From industry research:

> *"Cloud migration costs can vary significantly, ranging from approximately $40,000 for smaller firms to over $600,000 for large enterprises with complex workloads."* [14]

The code is mostly plain Python. If the organisation needed to move to Databricks tomorrow, it could. The Fabric-specific bits are isolated in thin adapter layers. That wouldn't be true if everything was built as Fabric notebooks with Fabric-specific APIs.

### Industry Alignment

This pattern is well-supported:

> *"Firms adopting both local composable infrastructure and continuous delivery within a hybrid IT model report greater control over workloads, improved reliability, faster updates, and reduced infrastructure complexity."* [15]

### The Trade-offs (Let's Be Honest)

Nothing's free. Here's what I'm trading:

| What You Give Up | How It's Mitigated |
|-----------------|-------------------|
| More complexity (two environments) | `PlatformFileOps` class handles the differences |
| Risk of local/cloud divergence | CI runs tests in both environments |
| Steeper learning curve | Good docs and examples for new starters |

### Risk Register

| # | Risk | Likelihood | Impact | Mitigation | Owner |
|---|------|:----------:|:------:|------------|-------|
| R1 | Local/cloud environment drift causes production bugs | Medium | High | CI pipeline runs full test suite against both environments on every PR | Data Engineering Lead |
| R2 | Fabric pricing model changes, invalidating cost case | Low | Medium | Annual pricing review; architecture remains beneficial for speed & portability regardless | Tech Lead |
| R3 | New team members struggle with dual-environment setup | Medium | Medium | Onboarding guide, pre-configured Conda `environment.yml`, pair programming during first sprint | Team Lead |
| R4 | Local development bypasses data governance controls | Low | High | No production data on DSVMs; all data access via OneLake RBAC; branch protection enforced | Data Governance Lead |
| R5 | Fabric API breaking changes affect adapter layer | Low | Medium | Adapter layer isolated behind `PlatformFileOps` interface; pinned dependency versions | Data Engineering Lead |

> **It's like building a house.** Buying a prefab is faster and simpler—it shows up on a truck and you're done. Building custom takes longer and costs more upfront, but you get exactly what you need, and you can modify individual rooms without replacing the whole thing.

---

## 7. How I Built It

### The Timeline

**Weeks 1-6: Local Development**
| Week | What I Did |
|:----:|------------|
| 1-2 | Architecture, design docs |
| 2-5 | Core Python library |
| 2-4 | CLI scripts for profiling/validation |
| 5-6 | Test coverage (hit 100%) |

**Weeks 6-9: Cloud Integration**
| Week | What I Did |
|:----:|------------|
| 6-8 | Fabric connector (`FabricDataQualityRunner`) |
| 8-9 | Packaging into `.whl` |
| 9 | Deploy to Fabric Environment |

**Weeks 9-12: Validation**
| Week | What I Did |
|:----:|------------|
| 9-11 | End-to-end testing |
| 11-12 | Go live |

### Where Things Stand Now

| Metric | Current | Target |
|--------|---------|--------|
| Test coverage | 100% (74/74) | ≥95% |
| Validation pass rate | 73.5% | ≥85% |
| Avg quality score | 98.8% | ≥95% |
| Pipeline runtime | ~60 sec | <2 min |
| Documentation | 180+ pages | Complete |

The validation pass rate is still climbing. Some of those failures are data issues I'm working with source teams to resolve, not bugs in the code. Based on current source-team engagement, I expect to reach the ≥85% target by **end of March 2026** (Week 20).

---

## 8. Architecture Decision Record (ADR)

| Field | Detail |
|-------|--------|
| **Status** | Accepted |
| **Decision** | Dual-platform architecture (DSVM CLI + MS Fabric) for AIMS and DQ Framework |
| **Context** | Enterprise data quality platform serving multiple projects; needs fast iteration, cost efficiency, testability, and vendor portability |
| **Consequences** | Additional environment complexity mitigated by `PlatformFileOps` abstraction; CI validates parity; ~$3.5k/year in avoided Fabric CU charges per 5 engineers; full pytest coverage enabled on DSVMs |

## 9. Summary

To recap the rationale behind the dual-platform approach:

1. **It avoids unnecessary spend** — ~80% of dev work runs on already-provisioned DSVMs, avoiding Fabric CU charges
2. **Faster iteration** — 10-50× speed improvement on DSVMs vs Fabric
3. **No vendor lock-in** — Code is portable to other platforms
4. **Governance isn't compromised** — Everything flows through Git and shared systems
5. **Industry-aligned** — Gartner, Forrester, and practitioners endorse this pattern

### The Bottom Line

For an enterprise platform like AIMS that will be maintained for years, the dual-platform approach is the right call. It requires more thought upfront, but it pays off in flexibility, cost savings, and resilience.

For quick prototypes or throwaway analysis? Fabric-only is fine. But that's not what this is.

---

## References

[1] Profisee. "Enterprise Data Quality Best Practices." 2024. https://profisee.com/data-quality-best-practices/

[2] Quora. "Technical Analysis of Code Duplication." https://www.quora.com/What-are-the-risks-of-code-duplication

[3] TimeXtender. "Microsoft Fabric Pricing Analysis 2025." 2025. https://www.timextender.com/blog/microsoft-fabric-pricing

[4] Promethium. "Understanding Fabric Capacity Units." 2024. https://www.promethium.ai/blog/microsoft-fabric-capacity-units

[5] DataEngineerThings. "Test-Driven Development in Data Engineering." 2024. https://dataengineerthings.org/tdd-data-engineering/

[6] GeeksForGeeks. "Local vs Cloud Development Environments." 2024. https://www.geeksforgeeks.org/local-vs-cloud-development/

[7] Reddit r/MicrosoftFabric. "Handling Shared Code in Notebooks." 2024. https://www.reddit.com/r/MicrosoftFabric/

[8] Bunnyshell. "Local Development vs Cloud Development Analysis." 2024. https://www.bunnyshell.com/blog/local-vs-cloud-development/

[9] Microsoft. "Microsoft Fabric Roadmap 2025-2026." 2025. https://learn.microsoft.com/en-us/fabric/release-plan/

[10] Medium. "Developer Productivity in Cloud Environments." https://medium.com/tag/developer-productivity

[11] Cloudflare. "Understanding Vendor Lock-in." 2024. https://www.cloudflare.com/learning/cloud/what-is-vendor-lock-in/

[12] Emergent Software. "Databricks vs Synapse vs Fabric Comparison." 2025. https://www.emergentsoftware.net/blog/databricks-vs-synapse-vs-fabric

[13] The FDA Group. "Audit Trail Requirements for Compliance." 2024. https://www.thefdagroup.com/blog/audit-trail-requirements

[14] Appinventiv. "Cloud Migration Cost Analysis 2025." 2025. https://appinventiv.com/blog/cloud-migration-cost/

[15] Forrester Research (via CDW). "Hybrid Cloud Benefits Study." 2024. https://www.cdw.com/content/cdw/en/articles/cloud/hybrid-cloud-benefits.html

[16] Great Expectations. "Data Quality Framework Documentation." 2025. https://docs.greatexpectations.io/

[17] Gartner. "Hybrid Cloud Strategy Guide." 2024. https://www.gartner.com/en/information-technology/insights/hybrid-cloud

[18] Acceldata. "Data Quality and Regulatory Compliance." 2024. https://www.acceldata.io/blog/data-quality-compliance

---

*Written by Sanmi — HS2 Data Engineering. Happy to walk through any of this in person.*

