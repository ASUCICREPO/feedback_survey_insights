import React, { useState, useRef } from "react";
import { Grid} from "@mui/material";
import { ThemeProvider } from "@mui/material/styles";
import { signOut } from "aws-amplify/auth"; // AWS Amplify for authentication
import AppHeader from "./Components/AppHeader";
import LeftNav from "./Components/LeftNav";
import ChatHeader from "./Components/ChatHeader";
import ChatBody from "./Components/ChatBody";
import UploadDialog from "./Components/Upload";
import Login from "./Components/LogInPage";
import theme from "./theme"; 
import { LanguageProvider } from "./utilities/LanguageContext";
import { TranscriptProvider } from './utilities/TranscriptContext';
import {Amplify} from 'aws-amplify';


function App() {
  const [uploadOpen, setUploadOpen] = useState(false); // State for managing the upload dialog
  const [isLoggedIn, setIsLoggedIn] = useState(() => {
    return localStorage.getItem("isLoggedIn") === "true";
  });


  Amplify.configure({
    Auth: {
      Cognito: {
        userPoolId: "us-east-1_xxxxxxx", // Your Cognito User Pool ID
        userPoolClientId: "xxxxxxxxxxxxxxxx", // Your App Client ID
        identityPoolId: "us-east-1:xxxxxxxxxxxxxxxx", // Optional: Identity Pool ID if using Federated Identities
        loginWith: {
          email: true, // Login using email
        },
        signUpVerificationMethod: "code", // Code-based verification during sign-up
        userAttributes: {
          email: {
            required: true, // Email is required as an attribute
          },
        },
        allowGuestAccess: false, // Set to true if guest access is needed
        passwordFormat: {
          minLength: 6,
          requireLowercase: true,
          requireUppercase: true,
          requireNumbers: true,
          requireSpecialCharacters: true,
        },
      },
    },
  });

  // Handle user login
  const handleLogin = () => {
    localStorage.setItem("isLoggedIn", "true");
    setIsLoggedIn(true);
  };

  // Handle user logout
  const handleLogout = async () => {
    try {
      await signOut(); // AWS Cognito sign out
      localStorage.removeItem("isLoggedIn");
      setIsLoggedIn(false);
    } catch (error) {
      console.error("Logout error", error);
    }
  };

  // Main application layout after login
  const MainApp = () => {
    const leftNavRef = useRef(); // Reference for LeftNav

    return (
      <Grid container direction="column" style={{ height: "100vh", overflow: "hidden" }}>
        <Grid item style={{ flexShrink: 0 }}>
          <AppHeader showSwitch={true} onUploadOpen={() => setUploadOpen(true)} onLogout={handleLogout} />
        </Grid>
        {uploadOpen ? (
          <Grid
            container
            item
            justifyContent="center"
            alignItems="center"
            style={{ flex: 1, backgroundColor: "#FFFFFF", zIndex: 1300 }}
          >
            <UploadDialog open={uploadOpen} onClose={() => setUploadOpen(false)} />
          </Grid>
        ) : (
          <Grid container item style={{ flex: 1, overflow: "hidden" }}>
            <Grid item xs={2.3} style={{ height: "100%", overflowY: "auto" }}>
              <LeftNav ref={leftNavRef} />
            </Grid>
            <Grid item xs={9} style={{ height: "100%", overflow: "hidden" }}>
              <Grid
                container
                item
                xs={12}
                direction="column"
                justifyContent="flex-start"
                alignItems="stretch"
                style={{ height: "100%", overflow: "hidden" }}
                sx={{
                  padding: { xs: "1.5rem", md: "1.5rem 5%", lg: "1.5rem 10%", xl: "1.5rem 10%" },
                  backgroundColor: (theme) => theme.palette.background.chatBody,
                }}
              >
                <Grid item style={{ flexShrink: 0 }}>
                  <ChatHeader />
                </Grid>
                <Grid item style={{ flex: 1, overflowY: "auto" }}>
                  <ChatBody leftNavRef={leftNavRef} />
                </Grid>
              </Grid>
            </Grid>
          </Grid>
        )}
      </Grid>
    );
  };

  // Render the Login component if not logged in, otherwise the main app
  return (
    <LanguageProvider>
      <TranscriptProvider>
        <ThemeProvider theme={theme}>
          {!isLoggedIn ? (
            <Login onLogin={handleLogin} /> // Render login page when not logged in
          ) : (
            <MainApp /> // Render main app after login
          )}
        </ThemeProvider>
      </TranscriptProvider>
    </LanguageProvider>
  );
}

export default App;