import { Component, EventEmitter, OnInit, Output,
  AfterViewInit,
  ElementRef } from '@angular/core';
import { BreakpointObserver, BreakpointState } from '@angular/cdk/layout';
import { Question } from '../model/question';
import { QuestionsService } from '../questions.service';

interface QuestionGroup {
  category: string;
  questions: Question[];
}

@Component({
  selector: 'app-page-result',
  standalone: false,
  templateUrl: './page-result.component.html',
  styleUrls: ['./page-result.component.css']
})
export class PageResultComponent implements OnInit, AfterViewInit {
  @Output() pageToShowEvent = new EventEmitter<number>();

  constructor(
    private questionsService: QuestionsService,
    private breakpointObserver: BreakpointObserver,
    private elementRef: ElementRef
  ) {}

  questions: Question[] = [];
  questionsByCategory: QuestionGroup[] = [];

  displayedColumns: string[] = [
    'text',
    'estimate',
    'comment'
  ];

  isMobile = false;

  ngOnInit(): void {
    this.breakpointObserver
      .observe(['(max-width: 768px)'])
      .subscribe((result: BreakpointState) => {
        this.isMobile = result.matches;
      });

    this.questionsService.questionsReplaySubject.subscribe(questions => {
      this.questions = questions;
      this.groupQuestions();
    });
  }

  ngAfterViewInit(): void {
    requestAnimationFrame(() => {
      const container = this.elementRef.nativeElement.closest(
        '.main-content'
      );

      if (container) {
        container.scrollTo({ top: 0, left: 0, behavior: 'instant' });
      } else {
        window.scrollTo({ top: 0, left: 0, behavior: 'instant' });
      }
    });
  }

  private groupQuestions(): void {
    const groups = new Map<string, Question[]>();

    for (const question of this.questions) {
      const category = question.category?.trim() || 'Без категории';

      if (!groups.has(category)) {
        groups.set(category, []);
      }

      groups.get(category)!.push(question);
    }

    this.questionsByCategory = Array.from(
      groups,
      ([category, questions]) => ({ category, questions })
    );
  }
}
