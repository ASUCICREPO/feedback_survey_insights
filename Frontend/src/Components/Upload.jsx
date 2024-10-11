import React, { useState } from "react";
import {
  Dialog, DialogContent, DialogActions, Button, IconButton, Grid, Typography, LinearProgress
} from "@mui/material";
import FolderIcon from "@mui/icons-material/Folder";
import CloseIcon from "@mui/icons-material/Close";
import { API_ENDPOINTS } from '../config';

const { INITIATE_UPLOAD_URL,COMPLETE_UPLOAD_URL,PROCESS_UPLOAD_URL } = API_ENDPOINTS;

const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunk size

function Upload({ open, onClose }) {
  const [file, setFile] = useState(null); // For holding the selected file
  const [uploadStatus, setUploadStatus] = useState(''); // Status feedback to the user
  const [uploadProgress, setUploadProgress] = useState(0); // For showing upload progress

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    setFile(selectedFile); // Set the selected file
    setUploadStatus(`Selected file: ${selectedFile.name}`); // Display the selected file name
  };

  const handleUpload = async () => {
    if (!file) {
      setUploadStatus("Please select a file to upload.");
      return;
    }

    try {
      // Reset progress
      setUploadProgress(0);
      setUploadStatus("Starting upload...");

      // Step 1: Initiate Multipart Upload
      const initiateResponse = await fetch(INITIATE_UPLOAD_URL, {
        method: 'POST',
        body: JSON.stringify({
          'Content-Type': 'application/octet-stream' 
        }),
      });

      if (!initiateResponse.ok) {
        throw new Error(`Failed to initiate upload: ${initiateResponse.statusText}`);
      }

      const responseData = await initiateResponse.json();
      const { uploadId,fileName } = responseData; // Destructure uploadId

      // Step 2: Split file into chunks
      const totalParts = Math.ceil(file.size / CHUNK_SIZE);
      const parts = Array.from({ length: totalParts }, (_, index) => index + 1);

      // Step 3: Get Pre-signed URLs for each part
      setUploadStatus("Starting Upload...");
      const presignResponse = await fetch(PROCESS_UPLOAD_URL, {
        method: 'POST',
        body: JSON.stringify({
          uploadId,
          parts,
          fileName
        }),
      });

      if (!presignResponse.ok) {
        throw new Error(`Failed to get presigned URLs: ${presignResponse.statusText}`);
      }

      const { presignedUrls } = await presignResponse.json();
      
      // Step 4: Upload each part and track progress
      setUploadStatus("Uploading file...");
      let uploadedParts = [];
      let completed = 0;

      for (let part of presignedUrls) {
        const { url, partNumber } = part;
        const start = (partNumber - 1) * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const blob = file.slice(start, end);

        const uploadPartResponse = await fetch(url, {
          method: 'PUT',
          body: blob
        });

        if (!uploadPartResponse.ok) {
          // console.log("teja upload error:",uploadPartResponse)
          throw new Error(`Failed to upload part ${partNumber}: ${uploadPartResponse.statusText}`);
        }

        // console.log(uploadPartResponse)
        uploadedParts.push({
          ETag: uploadPartResponse.headers.get('ETag'),
          PartNumber: partNumber
        });

        completed++;
        const percentCompleted = Math.round((completed / totalParts) * 100);
        setUploadProgress(percentCompleted); // Update progress
      }

      // Step 5: Complete Multipart Upload
      setUploadStatus("Finalizing upload...");
      const completeResponse = await fetch(COMPLETE_UPLOAD_URL, {
        method: 'POST',
        body: JSON.stringify({
          uploadId,
          parts: uploadedParts,
          fileName
        }),
      });

      if (!completeResponse.ok) {
        throw new Error(`Failed to complete upload: ${completeResponse.statusText}`);
      }

      setUploadStatus("Upload successful!");
      setUploadStatus("Please wait for 15 minutes for the new changes to take effect.");
      setUploadProgress(100); // Set progress to 100% after successful upload

    } catch (error) {
      console.error('Upload failed:', error);
      setUploadStatus(`Upload failed: ${error.message}`);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogContent
        sx={{
          padding: "2rem",
          position: "relative",
          backgroundColor: "#F4F5F5",
          borderRadius: "10px"
        }}
      >
        {/* Close Icon */}
        <IconButton
          aria-label="close"
          onClick={onClose}
          sx={{
            position: "absolute",
            right: 8,
            top: 8,
            color: "#003B5C", // Dark blue color for the close icon
          }}
        >
          <CloseIcon />
        </IconButton>
        {/* Header with Icon */}
        <Grid container alignItems="center" spacing={1} sx={{ marginBottom: "1.5rem" }}>
          <Grid item>
            <FolderIcon fontSize="large" sx={{ color: "#337AB7" }} /> {/* Light blue color for the icon */}
          </Grid>
          <Grid item>
            <Typography variant="h6" sx={{ color: "#003B5C", fontWeight: "bold" }}>
              Upload Dataset
            </Typography>{" "}
            {/* Dark blue and bold text */}
          </Grid>
        </Grid>
        {/* Drag and Drop Area */}
        <Grid
          container
          justifyContent="center"
          alignItems="center"
          sx={{
            border: "2px dashed #337AB7",
            padding: "2rem",
            borderRadius: "8px",
            textAlign: "center"
          }}
        >
          <Grid item>
            <Typography variant="body1" sx={{ color: "#003B5C" }}>
              Drag and drop your dataset here or
            </Typography>
            <Button
              variant="outlined"
              component="label"
              sx={{
                marginTop: "1rem",
                borderColor: "#003B5C", // Dark blue border color
                color: "#003B5C", // Dark blue text color
                borderRadius: "20px", // Rounded corners
                padding: "0.5rem 1.5rem",
                textTransform: "none",
              }}
            >
              Choose file
              <input type="file" hidden onChange={handleFileChange} />
            </Button>
            {file && (
              <Typography variant="body2" sx={{ color: "#003B5C", marginTop: "1rem" }}>
                {file.name}
              </Typography>
            )}
          </Grid>
        </Grid>

        {/* Show Upload Progress */}
        {uploadProgress > 0 && (
          <LinearProgress variant="determinate" value={uploadProgress} sx={{ marginTop: "1rem" }} />
        )}

        {/* Show Upload Status */}
        {uploadStatus && (
          <Typography variant="body2" sx={{ color: "#003B5C", marginTop: "1rem" }}>
            {uploadStatus}
          </Typography>
        )}
      </DialogContent>
      <DialogActions sx={{ padding: "1.5rem" }}>
        <Button
          variant="contained"
          fullWidth
          onClick={handleUpload}
          sx={{
            textTransform: "none",
            background: "linear-gradient(135deg, #337AB7 0%, #003B5C 100%)", /* Gradient for the button */
            padding: "0.75rem",
            color: "#FFFFFF", /* Ensure the text is white */
            borderRadius: "20px", /* Rounded corners for the button */
            boxShadow: "0px 4px 8px rgba(0, 0, 0, 0.1)", /* Slight shadow for depth */
          }}
        >
          Upload Dataset
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default Upload;
