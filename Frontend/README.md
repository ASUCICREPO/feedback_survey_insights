# 🌐 Frontend Setup for Feedback Survey Insights
Welcome to the Frontend of the Feedback Survey Insights project! This React-based application provides a seamless, interactive interface for users to engage with a Q&A bot, apply dynamic filters, and visualize meaningful insights derived from survey data.

## 🚀 Key Features
- Interactive Q&A Bot: Ask questions and receive insights based on processed survey data.
- Dynamic Filtering: Apply filters such as market, region, gender, and more to refine the data and customize your insights.
- Real-Time Insights & Summaries: Get detailed insights, actionable recommendations, and summaries for improved decision-making.

### 🛠️ Frontend Setup Guide

#### Step 1: Clone the Repository 🧑‍💻
Start by cloning the project repository from GitHub:

```bash
git clone https://github.com/ASUCICREPO/feedback_survey_insights.git
cd feedback_survey_insights/frontend
```

#### Step 2: Install Dependencies 📦
After navigating to the frontend directory, install the necessary dependencies using npm:

```bash
npm install
```

This will install all required packages, including AWS Amplify for authentication and API interaction.

#### Step 3: Configure AWS Cognito Authentication 🔐
In the App.js file, configure your AWS Cognito credentials to handle user authentication:

```bash
Amplify.configure({
  Auth: {
    userPoolId: "your_user_pool_id",        // Replace with your User Pool ID
    userPoolClientId: "your_client_id",     // Replace with your Client ID
    identityPoolId: "your_identity_pool_id" // Optional: Replace with Identity Pool ID
  },
});
```

Ensure that the credentials are configured correctly to allow users to sign in and interact with the application securely.

#### Step 4: Configure API Endpoints 🌐
In the `config.js` file, set up your API endpoints that will be used for processing and retrieving survey insights. Update the file with your API URLs:

```bash
export const API_ENDPOINTS = {
  START_PROCESSING_URL: 'https://your-api-url.com/start-processing', // API to initiate data processing
  GET_RESULTS_URL: 'https://your-api-url.com/get-results',           // API to retrieve results
  INITIATE_UPLOAD_URL: 'https://your-api-url.com/initiate-upload',   // API to initiate file uploads
  ...
};
```
Make sure you replace `your-api-url.com` with the actual URLs from the backend API outputs.

#### Step 5: Configure Filters 🎛️
In the `Config.js` file, define your filters for dynamic filtering in the frontend. Ensure that filter names are lowercase and contain no special characters `(except _ and -)` to be compatible with AWS Athena queries:

```bash
export const FILTERS = {
  Gender: ['M', 'F', 'U'],        // Gender filter options
  Department: ['Finance', 'Administration','Human Resource'],
  // Add more filters as necessary to match your dataset
};
```

This allows the frontend to dynamically adjust based on the available filters, offering users a customized experience.

#### Step 6: Customize Colors and Themes 🎨
To change the color schemes and themes, navigate to the constants.js file. You can modify the primary and secondary color variables to customize the look and feel of the application.

```bash
export const PRIMARY_COLOR = "#003B5C"; // Update to change the primary theme color
export const SECONDARY_COLOR = "#337AB7"; // Update to change the secondary theme color

export const BOTMESSAGE_BACKGROUND = "#F4F5F5";  // Background color for bot messages
export const USERMESSAGE_BACKGROUND = "#FFFFFF";  // Background color for user messages
```

Simply update the color values here to match your preferred theme. You can use any valid HEX color code or predefined CSS color values.

#### Step 7: Run the Frontend 🚀
To start the application locally, use the following command:

```bash
npm start
```

This will launch the frontend on http://localhost:3000, where you can interact with the Q&A bot and visualize insights in real time.

## ☁️ Deploying to AWS Amplify
Deploying the frontend to AWS Amplify is a great choice for scaling your application and making it accessible online with minimal setup.

#### Steps to Deploy via AWS Amplify:
Create an Amplify App:

Sign in to the AWS Amplify Console.
Choose Get Started under Host Your Web App.
Connect your GitHub repository (or another version control system).
Configure Build Settings: Amplify will automatically detect your build settings for a React app. If needed, you can customize the amplify.yml file:

```bash
version: 1
frontend:
  phases:
    preBuild:
      commands:
        - npm install
    build:
      commands:
        - npm run build
  artifacts:
    baseDirectory: /build
    files:
      - '**/*'
  cache:
    paths:
      - node_modules/**/*
```

#### Deploy:

- After setting up, Amplify will automatically build and deploy your application.
- You will be provided with a live URL to access the frontend.

##### Add Custom Domain (Optional):

In the Amplify Console, you can link a custom domain to your deployed application for a professional URL (e.g., www.yourdomain.com).

#### Why Use AWS Amplify?
- Fast Deployment: Amplify automates the entire deployment process, from build to hosting.
- Scalability: Easily handle large traffic without worrying about server management.
- Integrated with AWS: Seamlessly integrates with other AWS services like S3, Cognito and API Gateway for a complete serverless experience.

## 📝 Next Steps
Once your frontend is deployed and live on AWS Amplify, you can start collecting and analyzing feedback insights from users, refine filters and continue to improve the user experience!

## 🚀 Happy Building!
 Your journey to powerful insights and smooth user interactions starts here! ✨📊

