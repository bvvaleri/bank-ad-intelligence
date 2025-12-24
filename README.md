# Automated Bank Ad Intelligence with Tableau Cloud

## Overview
This project demonstrates how Tableau Cloud can be extended into an end-to-end analytics automation platform.  
It solves the problem of **competitive advertising intelligence** by automatically collecting, classifying, and publishing insights from public digital bank ads.

Instead of manual monitoring and static reports, the solution delivers **continuously updated analytics and proactive alerts** for marketing and strategy teams.

## End-to-End Architecture

![Architecture Diagram](assets/architecture.png)

---

## Problem
Banks run thousands of digital advertising campaigns across multiple channels.  
Tracking competitor messaging, campaign focus, and product positioning is typically manual, fragmented, and slow.

As a result:
- Insights arrive too late
- Trends are hard to quantify
- Decision-makers rely on incomplete data

---

## Solution
This project implements an automated pipeline that:
- Collects public Google Ads creatives using SerpAPI  
- Extracts **exact visible ad text** using AI-based OCR  
- Classifies campaigns into standardized banking product categories  
- Publishes a production-ready Hyper datasource to Tableau Cloud  
- Pushes **actionable summaries directly to Slack**

The result is a live, scalable analytics workflow that turns raw ads into business-ready insights.

---

## Architecture & Platform Usage
The pipeline is implemented as a cloud-native workflow and explicitly leverages Tableau’s developer platform:

- **Tableau Hyper API** is used to programmatically generate an analytics-ready `.hyper` datasource  
- **Tableau Server Client (TSC)** is used to automatically publish and refresh the datasource in Tableau Cloud  
- **Tableau Cloud** serves as the central analytics layer for exploration and insight sharing  
- **Slack Webhooks** are used to deliver proactive, actionable alerts to business users  

This architecture enables fully automated data ingestion, publishing, and insight delivery without manual intervention.

