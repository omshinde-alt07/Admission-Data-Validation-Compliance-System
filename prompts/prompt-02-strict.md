# Updated Candidate Processing Flow

## 1. Form Submission
Students fill out the admission form created using Tally.  
All responses are automatically stored in a Google Sheet.

---

## 2. Initial Validation and Segregation

A Python script runs and validates the candidate data based on rules defined in a **Config** sheet.

- Candidates who meet all criteria are moved to the **Clean Data** sheet.
- Candidates who do not meet the criteria are **rejected**.
- Special cases (for example, a candidate scoring 59.5% when the minimum requirement is 60%) are moved to a separate **Exception** sheet for manual review.

---

## 3. Exception Handling (Manual Review)

- A reviewer checks candidates listed in the **Exception** sheet.
- The reviewer adds a remark and updates the **Status** column (e.g., `Approved` or `Rejected`).
- When the Python script runs again, only records marked as **Approved** are moved to the **Clean Data** sheet.
- Until approval is given, their status remains **Pending**.

---

## 4. Test Score Integration

Another sheet contains **Test Scores** with the following fields:

- First Name  
- Last Name  
- Email ID  
- Phone Number  
- Test Score  

The Python script maps test scores to candidates in the **Clean Data** sheet using **Email ID** (common field in both sheets).

---

## 5. Final Screening for Next Round

- After mapping, validation is performed based on the test score criteria.
- Candidates who meet the required test score are moved to the **Interview** sheet.
- A **Status** column is added with the default value `Pending`.
