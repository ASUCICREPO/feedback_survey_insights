import React from "react";
import { Grid, AppBar, Button } from "@mui/material";

function AppHeader({ onUploadOpen, onLogout }) {
  return (
    <AppBar
      position="static"
      sx={{
        backgroundColor: (theme) => theme.palette.background.header,
        height: "6.5rem", // Adjusted height for a taller header
        boxShadow: "none",
        borderBottom: (theme) => `3.5px solid ${theme.palette.primary[50]}`, // Adjusted border thickness
        zIndex: 1301, // Ensure the header is above everything
      }}
    >
      <Grid
        container
        direction="row"
        justifyContent="space-between"
        alignItems="center"
        sx={{ padding: "0 2rem" }} // Adjust padding to reduce space between elements
        className="appHeight100"
      >
        <Grid item>
          <img src={"Insights"} alt="App main Logo" height={45} /> {/* Adjusted logo size for better visibility */}
        </Grid>
        <Grid item>
          <Grid container alignItems="center" spacing={2}>
            <Grid item>
              <Button
                variant="contained"
                sx={{ 
                  textTransform: "none", 
                  background: "linear-gradient(135deg, #337AB7 0%, #003B5C 100%)", // Gradient for the button
                  padding: "0.6rem 2rem",  // Adjust padding for a better fit
                  color: "#FFFFFF",  // White text color
                  borderRadius: "20px",  // Rounded corners
                  boxShadow: "0px 4px 8px rgba(0, 0, 0, 0.1)", // Slight shadow for depth
                }}
                onClick={onUploadOpen}
              >
                Upload Dataset
              </Button>
            </Grid>
            <Grid item>
              <Button 
                variant="outlined" 
                color="primary"
                sx={{ 
                  textTransform: "none",
                  borderColor: "#003B5C", // Dark blue border for consistency
                  color: "#003B5C", // Dark blue text color
                  padding: "0.6rem 1.5rem", // Adjust padding
                  borderRadius: "20px",  // Rounded corners
                }}
                onClick={onLogout} // Trigger logout
              >
                Log out
              </Button>
            </Grid>
          </Grid>
        </Grid>
      </Grid>
    </AppBar>
  );
}

export default AppHeader;
