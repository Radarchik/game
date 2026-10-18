export class Question {
  id: number;
  category: string
  text: string;
  estimate: number;
  comment: string;
  isOwn: boolean;
  answer: string;
  estimateAuthor: number;
  commentAuthor: string;
}

export class Category {
  id: number;
  name: string
}
