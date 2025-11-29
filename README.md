# Cyclistic 2024: Bike & E-Scooter Usage Analysis
Capstone project from the Coursera certification "Google Data Analytics Professional"

<img width="280" height="240" alt="image" src="https://github.com/user-attachments/assets/31acb175-e0e7-4404-897d-88c65ba73edf" />


## Google Data Analytics Capstone

This project is my capstone for the Google Data Analytics Professional Certificate.
This project is based on the 12 previous months of raw data (from January until December 2024 = 5.72 million rides). This project also required :

Handling the introduction of e-scooters in mid-2024

Performing full data cleaning, transformation, and visualization

Following the official data analysis cycle: Ask → Prepare → Process → Analyze → Share → Act

**Project Objective** : identify and understand behavioral differences between **annual members** (users with an annual membership) and **casual riders** (users without an annual membership).


## Business Objective

Cyclistic wants to increase the number of annual members.
To support a new marketing strategy, this analysis compares how casual riders and annual members use **classic bikes**, **electric bikes** and the newly introduced **electric scooters**.

## Dataset Overview

### Vehicle Types
- Classic bike
- Electric bike
- Electric scooter

### Station Data
- Start station name and ID
- End station name and ID

### Ride Data
- Start datetime
- End datetime
- Ride duration
- Month/day-of-week/hour (engineered features)

### User Type
- Member (User with membership)
- Casual (User without membership)
  → *No demographic or personnally identifying information*

## Key Insights

### 1. Weekly Ride Patterns

- Weekdays
  - Members ride significantly more than casual riders
  - Strong peak during commute hours

- Weekends
  - Casual rides increase
  - Member rides decrease

→ Suggests commuting behavior for members vs. leisure behavior for casual riders.

### 2. Seasonal Trends

- Cold months
  - Ride volume drops for both groups
  - Casual ridership drops the most

- Warm months
  - Sharp rise in ridership for both groups
  - Casual rides increase by +1200% from January → July
  - Member rides increase by +250%

→ Casual riders are highly weather-sensitive.

### 3. Ride Length Differences

- Casual riders have longer average ride durations

- Their average ride length increases on weekends

- Member ride duration is shorter and stable throughout the week

→ Members use Cyclistic mainly for transport, casual riders for leisure/tourism.

### 4. Station Usage Patterns

- Annual Members
  - Even distribution across top stations
  - Most frequently used stations located near workplaces

- Casual Riders
  - Uneven distribution
  - High concentration near tourist hotspots



## Recommendations to Convert Casual Riders into Members

### 1. Seasonal & Timing-Based Promotions

Target periods with high casual activity :

- Free summer membership trials
- Weekend-only discounts
- Good-weather notifications via the app

### 2. Location-Based Incentives

Focus on tourist-heavy stations :

- “Ride more, save more” offers near attractions
- Exclusive member discounts at tourist hotspots
- On-site promotional booths during peak season

### 3.  Bike-Type Incentives

Capitalize on casual riders’ preference for classic bikes :
- Unlimited 45-minute classic bike rides for members
- Classic-bike “tour bundles” as membership teasers
- Discounted day passes targeting casual leisure users

## Tools & Skills Used

SQL for EDA/Cleaning and Processing (BigQuery)

Python for EDA (Pandas, Matplotlib)

Data Cleaning (handling nulls, merging 12 monthly datasets)

Feature Engineering (ride length, day of week, ride type)

Data Visualization

Business analytics and recommendation writing
