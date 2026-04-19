# Big Data Analytics Lab – Week 14 (PySpark)

**St: Olga Durham** \
**St#: 040687883**

---

## Overview

This project demonstrates big data analytics using **PySpark** on a simulated retail transactions dataset.  
The lab covers the full analytics pipeline, including:

- Data preprocessing and feature engineering
- Descriptive analytics
- Diagnostic analytics
- Advanced analytics using window functions
- Predictive-style analysis (RFM scoring)
- Customer segmentation
- Anomaly detection
- Data export

---

## Dataset

The dataset is a **simulated retail transaction dataset** provided in the lab, containing:

- Customer information
- Product category
- Region
- Transaction timestamp
- Payment method
- Price and quantity

---

## Technologies Used

- Python 3.11
- PySpark
- Pandas
- Git & GitHub

---

## Data Preparation

### Figure 1 – Initial Schema

![Figure 1](screenshots/01-1-schema.png)

### Figure 2 – Sample Data (First 5 Rows)

![Figure 2](screenshots/01-1-show-table-5-rows.png)

### Figure 3 – Timestamp Conversion

![Figure 3](screenshots/02-schema-timestamp.png)

### Figure 4 – Revenue Column Added

![Figure 4](screenshots/03-schema-revenue.png)

---

## Descriptive Analytics

### Figure 5 – Summary Statistics

![Figure 5](screenshots/04-1-sum-statistics.png)

### Figure 6 – Revenue by Category

![Figure 6](screenshots/04-2-revenue-by-category.png)

### Figure 7 – Revenue by Region

![Figure 7](screenshots/04-3-revenue-by-region.png)

---

## Diagnostic Analytics

### Figure 8 – Revenue by Region and Category

![Figure 8](screenshots/05-1-revenue-by-region-and-category.png)

### Figure 9 – Revenue by Payment Method and Category (Pivot Table)

![Figure 9](screenshots/05-2-revenue-by-payment-method-and-category.png)

### Figure 10 – Monthly Revenue Trends

![Figure 10](screenshots/05-3-monthly-revenue-trends.png)

---

## Advanced Analytics (Window Functions)

### Figure 11 – Customer Revenue Ranking

![Figure 11](screenshots/06-1-customer-revenue-ranking.png)

### Figure 12 – Running Revenue by Region Over Time

![Figure 12](screenshots/06-2-running-revenue-by-region-over-time.png)

### Figure 13 – Revenue Quartiles

![Figure 13](screenshots/06-3-revenue-quartiles.png)

---

## Predictive Analytics (RFM Model)

### Figure 14 – RFM Scoring

![Figure 14](screenshots/07-rfm-scoring.png)

---

## Customer Segmentation

### Figure 15 – Customer Segmentation Results

![Figure 15](screenshots/08-customer-segmentation.png)

Segments identified:

- Champions
- Loyal Customers
- Potential Loyalists
- At Risk

---

## Anomaly Detection

### Figure 16 – Z-Score Based Anomaly Detection

![Figure 16](screenshots/09-anomaly-detection-z-score.png)

High-value transactions were identified as anomalies using statistical deviation.

---

## Data Export

### Figure 17 – Final Data Export

![Figure 17](screenshots/10-saving-data-using-pandas.png)

Due to Windows environment limitations related to Hadoop dependencies,  
the Spark DataFrame was converted to a Pandas DataFrame and exported as CSV:

```
output/final_transactions.csv
```

---

## Key Insights

- `Electronics` category generates the highest revenue
- `North` region performs best overall
- `credit_card` is the dominant payment method for high-value purchases
- `Alice` is the top customer (Champion segment)
- High-value transactions were successfully identified as anomalies

---

## Repository Structure

```
├── big_data_analytics_lab.py
├── screenshots/
├── output/
│ └── final_transactions.csv
├── README.md
```

---

## Reflection

This lab provided hands-on experience with PySpark for large-scale data processing and analytics. One of the key takeaways was understanding how different types of analytics: descriptive, diagnostic, and advanced can be applied to extract meaningful insights from raw data.

Working through the lab also highlighted practical challenges, particularly related to environment configuration on Windows. Issues such as Python version compatibility and Hadoop dependencies required troubleshooting and alternative solutions. This experience reinforced the importance of understanding the underlying system architecture when working with distributed data processing tools.

The implementation of RFM scoring and customer segmentation helped bridge the gap between technical analysis and real-world business applications. It demonstrated how data can be transformed into actionable insights for decision-making, such as identifying high-value customers and detecting unusual transactions.

Overall, this lab improved my confidence in using PySpark and deepened my understanding of how big data analytics workflows are structured and executed.

---

## Conclusion

In this lab, a complete big data analytics pipeline was developed using PySpark. Starting from raw transactional data, the process included data preparation, feature engineering, and multiple layers of analysis, including descriptive, diagnostic, and advanced analytics.

The results showed clear business insights, such as identifying top-performing regions, high-revenue product categories, and customer segments. The use of RFM scoring enabled effective customer classification, while anomaly detection highlighted unusual high-value transactions.

Despite technical limitations in the Windows environment, a successful workaround was implemented by exporting the final dataset using Pandas. This demonstrated adaptability in handling real-world constraints.

In conclusion, this lab illustrated the practical application of PySpark in analyzing and interpreting large datasets, providing valuable experience in both technical implementation and analytical thinking.

This lab reflects real-world data engineering and analytics workflows commonly used in industry environments.
