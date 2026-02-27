# Research Notes – Vibe Coding

**Date:** Wednesday Evening
**Project Context:** Candidate Processing & Automation System

---

## 1. YouTube Learning Resources

### Video 1

**Title:** Vibe Coding / AI Workflow Concept
Link: https://youtu.be/SordE3oOSyc?si=sjavT_OcogeWcUsZ

**Key Learnings**

* Vibe coding focuses on building systems quickly using AI assistance instead of traditional long development cycles.
* The approach emphasizes:

  * Rapid prototyping
  * Iterative improvement
  * Using AI for code generation, debugging, and system design
* Best suited for automation workflows, internal tools, and MVPs.

---

### Video 2

Link: https://youtu.be/iLCDSY2XX7E?si=y5YIQynk4ZQT8koG

**Key Learnings**

* AI tools can handle:

  * Data processing
  * Validation logic
  * Workflow automation
* Importance of:

  * Clear system prompts
  * Structured data (Google Sheets, CSV, APIs)
* Human role shifts from coding to **system thinking and orchestration**.

---

### Video 3

Link: https://youtu.be/Tw18-4U7mts?si=VYuGu8GFc9beM1Jx

**Key Learnings**

* Build end-to-end workflows using:

  * Forms → Data Storage → Processing → Output
* Focus on:

  * Automation pipelines
  * No-code/low-code integrations
* Recommended approach:

  * Start simple
  * Automate repetitive tasks
  * Add intelligence layer later.

---

## 2. Google Sheets Integration Research

Explored how Google Sheets can act as a lightweight database for automation.

**What was studied**

* Reading/writing data via APIs
* Using Sheets as:

  * Data storage
  * Config management
  * Status tracking
* Integration tools explored:

  * Google Apps Script
  * Python (gspread / Google API)
  * AI workflow tools

**Tools Explored**

* Lovable AI – for rapid UI + workflow generation
  Link: https://lovable.ai
* Google AI Studio – for prompt testing and AI integration
  Link: https://aistudio.google.com

---

## 3. Tally Form Documentation

Studied official documentation for form creation and data collection.

**Key Points**

* Easy form builder with shareable links
* Direct integration with Google Sheets
* Suitable for candidate data collection
* Supports structured responses required for automation

Official Documentation:
https://tally.so/help

---

## 4. Relevance to Our Project

This research helped define the project architecture:

**Workflow Inspired by Vibe Coding**

1. Candidate fills **Tally Form**
2. Responses stored in **Google Sheets**
3. Python script processes data:

   * Validate based on Config Sheet
   * Separate:

     * Valid candidates
     * Rejected candidates
     * Exception cases (e.g., 59.5% vs 60%)
4. Status updated automatically

---

## 5. Key Takeaways

* Focus on **speed over perfection** (build → test → improve)
* Use **Google Sheets as backend**
* Keep logic configurable instead of hardcoded
* Combine:

  * No-code (Tally)
  * Low-code (Sheets)
  * AI/Python automation

---

## 6. Next Steps

* Implement Google Sheets validation script
* Create Exception handling workflow
* Add status tracking (Pending / Approved / Rejected)
* Explore AI-based candidate scoring in future
