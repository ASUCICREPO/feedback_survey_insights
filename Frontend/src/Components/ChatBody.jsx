import React, { useState, useRef, useEffect } from "react";
import { Grid, Avatar, Typography, Box,CircularProgress } from "@mui/material";
import ChatInput from "./ChatInput";
import UserAvatar from "../Assets/UserAvatar.svg";
import StreamingResponse from "./StreamingResponse";
import createMessageBlock from "../utilities/createMessageBlock";
import { ALLOW_FAQ } from "../utilities/constants";
import { FAQExamples } from "./index";

function ChatBody({ leftNavRef }) {
  const [messageList, setMessageList] = useState([]);
  const [processing, setProcessing] = useState(false);
  const [message, setMessage] = useState("");
  const [questionAsked, setQuestionAsked] = useState(false); 
  const messagesEndRef = useRef(null);

  useEffect(() => {
    scrollToBottom();
  }, [messageList]);

  useEffect(() => {
    scrollToBottom();
  }, [processing]);

  const scrollToBottom = () => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  };

  const handleSendMessage = (message) => {
    setProcessing(true); 
    const newMessageBlock = createMessageBlock(message, "USER", "TEXT", "SENT");
    setMessageList([...messageList, newMessageBlock]);
    getBotResponse(setMessageList, setProcessing, message, leftNavRef);
    setQuestionAsked(true); 
  };

  const handlePromptClick = (prompt) => {
    handleSendMessage(prompt); 
  };

  return (
    <Box display="flex" flexDirection="column" justifyContent="space-between" className="appHeight100 appWidth100" sx={{ width:'fit-content+50px'}}>
      <Box flex={1} overflow="auto" className="chatScrollContainer">
        <Box sx={{ display: ALLOW_FAQ ? "flex" : "none" }}>
          {!questionAsked && <FAQExamples onPromptClick={handlePromptClick} />}
        </Box>
        {messageList.map((msg, index) => (
          <Box key={index} mb={2}>
            {msg.sentBy === "USER" ? (
              <UserReply message={msg.message} />
            ) : (
              <StreamingResponse
                initialMessage={msg.message}
                setProcessing={setProcessing}
                leftNavRef={leftNavRef} // Pass ref to get filters from LeftNav
                scrollToBottom={scrollToBottom}
              />
            )}
          </Box>
        ))}
        {processing && (
          <Box display="flex" justifyContent="center" mt={2}>
            <CircularProgress sx={{ color: "#003B5C" }} />
          </Box>
        )}
        <div ref={messagesEndRef} />
      </Box>

      <Box display="flex" justifyContent="space-between" alignItems="flex-end" sx={{ flexShrink: 0 }}>
        <Box sx={{ width: "100%" }} ml={2}>
          <ChatInput onSendMessage={handleSendMessage} processing={processing} message={message} setMessage={setMessage} />
        </Box>
      </Box>
    </Box>
  );
}

export default ChatBody;

function UserReply({ message }) {
  return (
    <Grid container direction="row" justifyContent="flex-end" alignItems="flex-end">
      <Grid item className="userMessage" sx={{ backgroundColor: (theme) => theme.palette.background.userMessage }}>
        <Typography variant="body2">{message}</Typography>
      </Grid>
      <Grid item>
        <Avatar alt={"User Profile Pic"} src={UserAvatar} />
      </Grid>
    </Grid>
  );
}

const getBotResponse = (setMessageList, setProcessing, message, leftNavRef) => {
  const botMessageBlock = createMessageBlock(message, "BOT", "TEXT", "PROCESSING");
  setMessageList((prevList) => [...prevList, botMessageBlock]);
  // The API request and response handling are done in the StreamingResponse component
};
