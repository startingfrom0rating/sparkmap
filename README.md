# ✨ Spark Map

A geospatial intelligence platform for advocacy, visualizing opportunity data across Maryland census tracts.

![Spark Map](https://img.shields.io/badge/Mapbox-GL%20JS-blue) ![Data](https://img.shields.io/badge/Data-Opportunity%20Atlas-green)

## 🎯 Features

- **6 Data Lenses** — Income Mobility, Opportunity Index, Education, Health, Social, Incarceration
- **Mobility Deserts** — Highlight tracts with income mobility < 40%
- **Address Search** — Find any Maryland location
- **County Filter** — Focus on specific counties with animated stats
- **POI Layers** — Toggle hospitals, schools, parks, libraries, stores
- **CSV Export** — Download desert tracts for advocacy reports

## 🚀 Quick Start

```bash
cd mapbox_ready
python -m http.server 8080
```

Open **http://localhost:8080**

## 📁 Project Structure

```
sparkmap/
├── mapbox_ready/           # Web application
│   ├── index.html          # Main app
│   ├── maryland_tracts_with_scores.geojson
│   ├── hospitals.geojson
│   ├── schools.geojson
│   ├── parks.geojson
│   ├── libraries.geojson
│   └── stores.geojson
├── data/                   # Source datasets
└── *.py                    # Data preparation scripts
```

## 📊 Data Sources

- [Opportunity Atlas](https://opportunityatlas.org/) — Income mobility metrics
- [Child Opportunity Index 3.0](https://www.diversitydatakids.org/) — Education, health, social domains
- [Census Bureau](https://www.census.gov/) — Tract boundaries

## 🗺️ Deployment

For production, upload the `mapbox_ready/` folder to any static host:
- GitHub Pages
- Netlify
- Vercel

## 📝 License

MIT
