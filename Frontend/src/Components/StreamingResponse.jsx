import React, { useState, useEffect } from "react";
import { Grid, Typography, Avatar, CircularProgress, Button, Collapse, Alert } from "@mui/material";
import icon from '../Assets/icon.png'; // Import the company logo
import { API_ENDPOINTS } from '../config';

const { START_PROCESSING_URL, GET_RESULTS_URL } = API_ENDPOINTS;

const StreamingMessage = ({ initialMessage, setProcessing, leftNavRef, scrollToBottom }) => {
  // State variables
  const [jobId, setJobId] = useState(null);
  const [ setExecutionArn] = useState(null);
  const [setLoading] = useState(false);
  const [polling, setPolling] = useState(false);
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [setVisibleSampleRows] = useState({}); // Track visibility of sample rows

  // Function to start the processing job
  const startProcessingJob = async () => {
    setLoading(true);
    setError(null);
    setProcessing(true)
    scrollToBottom(); // Scroll to the bottom when loading starts

    try {
      const filters = leftNavRef.current.getTrueFilters();

      const response = await fetch(START_PROCESSING_URL, {
        method: 'POST',
        mode: 'cors',
        body: JSON.stringify({
          query: initialMessage,
          filters: filters,
        }),
      });

      const jsonResponse = await response.json();

      if (!response.ok) {
        // Handle validation errors or other errors returned by the API
        throw new Error(jsonResponse.error || 'Failed to start processing job.');
      }

      const { job_id, execution_arn } = jsonResponse;
      setJobId(job_id);
      setExecutionArn(execution_arn);
      setPolling(true); // Start polling for results
    } catch (err) {
      console.error('Error:', err);
      setError(err.message || 'An unexpected error occurred.');
      setProcessing(false);
    } finally {
      setLoading(false);
      //setProcessing(false);
      scrollToBottom(); // Scroll to the bottom after loading is complete
    }
  };

  // Function to fetch the job results
  const fetchJobResults = async (currentJobId) => {
    try {
      const response = await fetch(`${GET_RESULTS_URL}?jobId=${currentJobId}`, {
        method: 'GET',
        mode: 'cors',
      });

      const jsonResponse = await response.json();

      if (!response.ok) {
        // Handle errors returned by the API
        throw new Error(jsonResponse.error || 'Failed to fetch job results.');
      }

      const { status, output, error: jobError, cause } = jsonResponse;

      if (status === 'SUCCEEDED') {
        // const lambda2Body = JSON.parse(output.lambda2_result.body);
        setData(output);
        setPolling(false);
        setProcessing(false);
      } else if (status === 'FAILED') {
        throw new Error(`${jobError || 'Job failed.'}${cause ? ` Cause: ${cause}` : ''}`);
      }
      // If status is RUNNING or other, do nothing and continue polling
    } catch (err) {
      console.error('Error fetching job results:', err);
      setError(err.message || 'An unexpected error occurred while fetching results.');
      setPolling(false);
      setProcessing(false);
    }
  };

  // useEffect to start the polling when polling state is true
  useEffect(() => {
    let intervalId;

    if (polling && jobId) {
      intervalId = setInterval(() => {
        fetchJobResults(jobId);
      }, 10000); // Poll every  seconds
    }

    return () => {
      if (intervalId) clearInterval(intervalId);
    };
  }, [polling, jobId]);

  // Handle initial processing job initiation
  useEffect(() => {
    if (initialMessage) {
      startProcessingJob();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMessage]);

  // Function to toggle visibility of sample rows
  const toggleSampleRow = (index) => {
    setVisibleSampleRows(prevState => ({
      ...prevState,
      [index]: !prevState[index]
    }));
  };

  
  if (error) {
    return (
      <Grid container direction="column" sx={{ padding: "1rem", borderRadius: "10px", backgroundColor: "#FFE6E6", marginBottom: "1rem" }}>
        <Alert severity="error">{error}</Alert>
      </Grid>
    );
  }

  // If data is available, display the insights and summary
  if (data) {
    return (
      <Grid container direction="column" sx={{ padding: "1rem", borderRadius: "10px", backgroundColor: "#F4F5F5", marginBottom: "1rem", position: "relative" }}>
        {/* Icon positioning fixed here */}
        <Grid item container alignItems="center" spacing={2} sx={{ marginBottom: '1rem' }}>
          <Grid item>
            <Avatar 
              alt="Company Logo" 
              src={icon} 
              sx={{ width: 40, height: 40, backgroundColor: "#F4F5F5", border: "2px solid #003B5C" }} 
            />
          </Grid>
          <Grid item>
            <Typography variant="h6" sx={{ color: "#003B5C", fontWeight: "bold" }}>Insights</Typography>
          </Grid>
        </Grid>

        <Grid item>
          <ol style={{ paddingLeft: "1rem", color: "#003B5C", fontSize: "1rem" }}>
            {data.insights.map((item, index) => (
              <li key={index} style={{ marginBottom: "2rem", color: "#003B5C" }}>
                <Typography variant="body1" sx={{ fontWeight: "bold" }}>{item.insight}</Typography>
                <Typography variant="body2" sx={{ marginLeft: "1rem", color: "#337AB7", marginTop: "0.5rem" }}>
                  <em>Recommendation:</em> {item.recommendation}
                </Typography>
              </li>
            ))}
          </ol>
        </Grid>

        <Grid item>
          <Typography variant="body1" sx={{ marginTop: "2rem", fontWeight: "bold", color: "#003B5C" }}>Overall Summary:</Typography>
          <Typography variant="body2" sx={{ marginTop: "0.5rem", color: "#003B5C" }}>{data.summary}</Typography>
        </Grid>
      </Grid>
    );
  }

  // If no data and no error, render nothing or a placeholder
  return null;
};

export default StreamingMessage;