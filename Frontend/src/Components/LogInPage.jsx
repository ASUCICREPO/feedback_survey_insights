import React, { useState } from "react";
import { Grid, TextField, Button, Typography } from "@mui/material";
import { signIn } from 'aws-amplify/auth'; // Import the Auth module from Amplify

function Login({ onLogin }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setErrorMessage('');
    
    try {
      // AWS Cognito login
      const user = await signIn({
        username: email,
        password: password,
      })
      
      onLogin(); // Call the onLogin function when successful
    } catch (error) {
      console.error('Login error', error);
      setErrorMessage('Invalid credentials! Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Grid container style={{ height: "100vh" }}>
      <Grid
        item
        xs={5}
        style={{
          background: "linear-gradient(135deg, #337AB7 0%, #003B5C 100%)",
        }}
      />
      <Grid
        item
        xs={7}
        container
        justifyContent="center"
        alignItems="center"
        style={{ backgroundColor: "#F4F5F5" }}
      >
        <Grid item style={{ width: 350 }}>
          <form onSubmit={handleSubmit}>
            <Grid container direction="column" spacing={3}>
              <Grid item>
                <img
                  alt="Logo"
                  style={{ maxWidth: "100%" }}
                />
              </Grid>
              <Grid item>
                <TextField
                  fullWidth
                  label="Email address"
                  type="email"
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  variant="outlined"
                />
              </Grid>
              <Grid item>
                <TextField
                  fullWidth
                  label="Password"
                  type="password"
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  variant="outlined"
                />
              </Grid>
              {/* Display error message if it exists */}
              {errorMessage && (
                <Grid item>
                  <Typography
                    variant="body2"
                    style={{ color: "red", textAlign: "center" }}
                  >
                    {errorMessage}
                  </Typography>
                </Grid>
              )}
              <Grid item>
                <Button
                  type="submit"
                  variant="contained"
                  fullWidth
                  disabled={loading}
                  sx={{
                    textTransform: "none",
                    background: "linear-gradient(135deg, #337AB7 0%, #003B5C 100%)",
                    padding: "0.75rem",
                    color: "#FFFFFF",
                    borderRadius: "20px",
                  }}
                >
                  {loading ? "Logging in..." : "Login"}
                </Button>
              </Grid>
            </Grid>
          </form>
        </Grid>
      </Grid>
    </Grid>
  );
}

export default Login;
