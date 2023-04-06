def process_message_ml(msg, sentiment_task, classifier):
    # Truncate the message if it's too long for the model
    max_length = 428  # This may vary depending on the model
    truncated_message = msg['message'][:max_length]

    # Process the truncated message with sentiment analysis and emotion classification models
    sentiment_result = sentiment_task(truncated_message)
    emotion_result = classifier(truncated_message)

    # Add results to the message dictionary
    msg['sentiment'] = sentiment_result[0]['label']
    msg['sentiment_score'] = sentiment_result[0]['score']

    # get emotion and score with max score
    max_emotion = max(emotion_result[0], key=lambda x: x['score'])
    msg['emotion'] = max_emotion['label']
    msg['emotion_score'] = max_emotion['score']

    return msg
