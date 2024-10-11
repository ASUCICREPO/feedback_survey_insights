import React, { useState, useImperativeHandle, forwardRef } from "react";
import { Paper, Typography, Checkbox, FormControlLabel, FormGroup, FormControl, MenuItem, Select } from "@mui/material";
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import './LeftNav.css';
import { FILTERS } from '../config.js';

const LeftNav = forwardRef((props, ref) => {
  const [expanded, setExpanded] = useState([]);
  const [filters, setFilters] = useState(() => {
    const initialFilters = {};
    for (const category in FILTERS) {
      initialFilters[category] = {};
      FILTERS[category].forEach((item) => {
        initialFilters[category][item] = false;
      });
    }
    return initialFilters;
  });

  // Mapping for frontend display names
  const filterMapping = {
    "Department": 'Department',
    Gender: 'Gender',
    Ethnicity: 'Ethnicity',
  };

  useImperativeHandle(ref, () => ({
    getTrueFilters: () => {
      const trueFilters = {};
      for (const category in filters) {
        for (const key in filters[category]) {
          if (filters[category][key]) {
            if (!trueFilters[category]) {
              trueFilters[category] = [];
            }
            trueFilters[category].push(key);
          }
        }
      }
      return Object.keys(trueFilters).map(category => ({
        [category]: trueFilters[category],
      }));
    }
  }));

  const handleFilterChange = (category, value) => {
    setFilters((prev) => ({
      ...prev,
      [category]: {
        ...prev[category],
        [value]: !prev[category][value],
      },
    }));
  };

  const handleExpand = (panel) => {
    setExpanded((prev) => {
      const newExpanded = prev.includes(panel)
        ? prev.filter(item => item !== panel)
        : [...prev, panel];

      // Trigger bounce effect on the line
      const hrElement = document.getElementById(`${panel}-hr`);
      if (hrElement) {
        hrElement.classList.remove("bounce");
        void hrElement.offsetWidth; // Trigger reflow to restart animation
        hrElement.classList.add("bounce");
      }

      return newExpanded;
    });
  };

  const renderFilters = (categoryKey) => {
    const displayCategory = Object.keys(filterMapping).find(
      (key) => filterMapping[key] === categoryKey
    );

    return (
      <div key={categoryKey} className="filter-category">
        <div className="filter-header" onClick={() => handleExpand(categoryKey)}>
          <Typography sx={{ fontWeight: 'bold' }}>{displayCategory}</Typography>
          <ExpandMoreIcon
            sx={{
              transform: expanded.includes(categoryKey) ? 'rotate(180deg)' : 'rotate(0deg)',
              transition: 'transform 0.3s',
            }}
          />
        </div>
        
        <div className={`filter-options ${expanded.includes(categoryKey) ? "options-expanded" : ""}`}>
          <FormGroup>
            {Object.keys(filters[categoryKey]).map((value) => (
              <FormControlLabel
                key={value}
                control={
                  <Checkbox
                    checked={filters[categoryKey][value]}
                    onChange={() => handleFilterChange(categoryKey, value)}
                  />
                }
                label={value}
              />
            ))}
          </FormGroup>
        </div>
        
        <hr id={`${categoryKey}-hr`} className="bounce" />
      </div>
    );
  };

  return (
    <Paper
      sx={{
        backgroundColor: '#F4F5F5',
        borderRight: (theme) => `3.5px solid ${theme.palette.primary[50]}`,
        height: "100vh",
        boxShadow: 'none',
        padding: '0',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div style={{ flexGrow: 1, overflowY: 'auto', padding: '6px 50px 2px 30px' }}>
        {Object.keys(filters).map((category) => renderFilters(category))}
      </div>
    </Paper>
  );
});

export default LeftNav;
