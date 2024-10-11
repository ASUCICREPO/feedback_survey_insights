// --------------------------------------------------------------------------------------------------------//
// Primary color constants for the theme
export const PRIMARY_MAIN = "#000000"; // The main primary color used for buttons, highlights, etc.
export const primary_50 = "#4F5BA6"; // The 50 variant of the primary color

// Background color constants
export const SECONDARY_MAIN = "#D3D3D3"; // The main secondary color used for less prominent elements

// Chat component background colors
export const CHAT_BODY_BACKGROUND = "#"; // Background color for the chat body area
export const CHAT_LEFT_PANEL_BACKGROUND = "#F4F5F5"; // Background color for the left panel in the chat
export const ABOUT_US_HEADER_BACKGROUND = "#FFFFFF"; // Background color for the About Us section in the left panel
export const FAQ_HEADER_BACKGROUND = "#F4F5F5"; // Background color for the FAQ section in the left panel
export const ABOUT_US_TEXT = "#FFFFFF"; // Text color for the About Us section in the left panel
export const FAQ_TEXT = "#F4F5F5"; // Text color for the FAQ section in the left panel
export const HEADER_BACKGROUND = "#F4F5F5"; // Background color for the header
export const HEADER_TEXT_GRADIENT = "#444E56"; // Text gradient color for the header

// Message background colors
export const BOTMESSAGE_BACKGROUND = "#F4F5F5"; // Background color for messages sent by the bot
export const USERMESSAGE_BACKGROUND = "#F4F5F5"; // Background color for messages sent by the user

// --------------------------------------------------------------------------------------------------------//
// --------------------------------------------------------------------------------------------------------//

// Text Constants
export const TEXT = {
  EN: {
    APP_NAME: "Chatbot Template App",
    APP_ASSISTANT_NAME: "Feedback Survey Insights",
    FAQ_TITLE: "Frequently Asked Questions",
    FAQS: [
      "What are the top insights for the feedback provided?",
      "What are the positive insights in the feedback provided?",
      "What are the negative insights in the feedback provided?",
      "What are the improvement opportunities in the feedback provided?"
    ],
    // CHAT_HEADER_TITLE: "Sample AI Chat Assistant",
    CHAT_INPUT_PLACEHOLDER: "Type a new question",
    HELPER_TEXT: "Cannot send empty message",
    SPEECH_RECOGNITION_START: "Start Listening",
    SPEECH_RECOGNITION_STOP: "Stop Listening",
    SPEECH_RECOGNITION_HELPER_TEXT: "Stop speaking to send the message" // New helper text
  },
};

// --------------------------------------------------------------------------------------------------------//
// --------------------------------------------------------------------------------------------------------//

// API endpoints


export const CHAT_API = process.env.REACT_APP_CHAT_API; // URL for the chat API endpoint
export const WEBSOCKET_API = process.env.REACT_APP_WEBSOCKET_API; // URL for the WebSocket API endpoint


// --------------------------------------------------------------------------------------------------------//
// --------------------------------------------------------------------------------------------------------//

// Features
export const ALLOW_FILE_UPLOAD = false; // Set to true to enable file upload feature
export const ALLOW_VOICE_RECOGNITION = false; // Set to true to enable voice recognition feature

export const ALLOW_MULTLINGUAL_TOGGLE = false; // Set to true to enable multilingual support
export const ALLOW_LANDING_PAGE = true; // Set to true to enable the landing page

// --------------------------------------------------------------------------------------------------------//
// Styling under work, would reccomend keeping it false for now
export const ALLOW_MARKDOWN_BOT = false; // Set to true to enable markdown support for bot messages
export const ALLOW_FAQ = true; // Set to true to enable the FAQs to be visible in Chat body 