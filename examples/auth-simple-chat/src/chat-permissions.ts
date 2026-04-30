export type ChatMessageInput = {
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
};

export function chatMessageInput(
  chatId: string,
  authorName: string,
  text: string,
): ChatMessageInput {
  return {
    author_name: authorName,
    chat_id: chatId,
    text,
    sent_at: new Date(),
  };
}
