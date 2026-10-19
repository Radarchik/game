import {AfterViewInit, ChangeDetectorRef, Component, EventEmitter, OnInit, Output, ViewChild, ViewEncapsulation} from '@angular/core';
import {MatDialog} from '@angular/material/dialog';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import {MatTableDataSource} from '@angular/material/table';
import { HttpClient } from '@angular/common/http';
import {Question} from '../model/question';
import {MatSnackBar} from '@angular/material/snack-bar';
import {CommentDialogComponent} from '../comment-dialog/comment-dialog.component';
import {QuestionsService} from '../questions.service';
import {map} from 'rxjs';
import {BreakpointObserver, BreakpointState} from '@angular/cdk/layout';

@Component({
  selector: 'app-page2',
  standalone: false,
  templateUrl: './page2.component.html',
  encapsulation: ViewEncapsulation.None,
  styleUrls: ['./page2.component.css']
})
export class Page2Component implements OnInit, AfterViewInit  {
  @Output() pageToShowEvent = new EventEmitter<number>();
  dataSource: MatTableDataSource<Question>;
  questions: Question[];
  isLoad = false;

  displayedColumns: string[] = ['number', 'name', 'estimate', 'comment'];

  constructor(
    public dialog: MatDialog,
    private snackBar: MatSnackBar,
    private httpClient: HttpClient,
    private questionsService: QuestionsService,
    private breakpointObserver: BreakpointObserver,
    private cdr: ChangeDetectorRef ) { }

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;
  isMobile: boolean;

  ngAfterViewInit() {
  }

  ngOnInit() {
    this.breakpointObserver
      .observe(['(max-width: 768px)'])
      .subscribe((result: BreakpointState) => {
        this.isMobile = result.matches;
      });

    this.questionsService.questionsReplaySubject.subscribe(questions => {
      console.log(questions);
      this.questions = questions;
      setTimeout(() => {
        this.dataSource = new MatTableDataSource(questions);
        this.dataSource.paginator = this.paginator;
      }, 0);
    });
  }

  sendResult() {
    const commonQuestions = this.questions.filter(q => q.isOwn === false);
    const canContinue = commonQuestions.every(question => (question.estimate || question.answer));
    if (canContinue) {
      this.resetOrder();
      this.questionsService.questionsReplaySubject.next(this.questions);
      const body = JSON.stringify(this.questions);
      this.isLoad = true
      this.httpClient.post('https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net', body)
        .subscribe(response => {
          this.isLoad = false
          if (response) {
            this.pageToShowEvent.emit(3);
          }
        });
    } else {
      this.openSnackBar();
    }
  }

  onValChange(value: any, question: Question) {
    question.estimate = value;
  }

  openSnackBar() {
    const unsignedQ = this.questions.filter(question => (!question.isOwn && (!question.estimate && !question.answer)));
    const message = 'Остались вопросы: ' + unsignedQ.map(q => q.id);
    const action = 'Attention';
    this.snackBar.open(message, action, {
      duration: 3000,
    });
  }

  goToComment(question: Question) {
    this.openDialog(question);
  }

  openDialog(question: Question) {
    let commentDialogRef = this.dialog.open(CommentDialogComponent, {
      disableClose: true,
      data: question.comment
    });
    commentDialogRef.afterClosed().subscribe(result => {
      if (result || result === '') {
        question.comment = result;
        this.cdr.detectChanges();
      }
      commentDialogRef = null;
    });
  }

  goToPage1() {
    this.questionsService.questionsReplaySubject.next(this.questions);
    this.pageToShowEvent.emit(1);
  }

  onInputChange(value: any, question: Question) {
    question.text = value;
  }

  onInputNotToggleChange(value: any, question: Question) {
    question.answer = value;
  }

  resetOrder(): void {
    // Метод .sort() мутує поточний масив, сортуючи за зростанням id
    this.questions = this.questions
      .filter(q => q.text)
      .sort((a, b) => a.id - b.id);

    const jsonOwn =   [{
        "id": 43,
        "category": "Собственные фантазии (опционально)",
        "text": "Ну есть такая фантазия, чтобы побыть реальным рабом Госпожи на день/два или больше, чтобы должен выполнять прихоти и приказы, не только в рамках темы, а в быту и т.п. (в рамках табу)",
        "commentAuthor": "",
        "estimateAuthor": 4,
        "isSituation": false
      },
      {
        "id": 44,
        "category": "Собственные фантазии (опционально)",
        "text": "а вобще фантазий много, но я сегодня спешу сделать эту штуку и все повылетало с головы",
        "commentAuthor": "",
        "estimateAuthor": 4,
        "isSituation": false
      },
      {
        "id": 45,
        "category": "Собственные фантазии (опционально)",
        "text": "ах да, самая главная фантазия - выполнять фантазии Госпожи)",
        "commentAuthor": "",
        "estimateAuthor": 4,
        "isSituation": false
      }];

    const nextId = this.questions.length > 0
      ? Math.max(...this.questions.map(q => q.id)) + 1
      : 1;

    const additionalQuestions = jsonOwn.map((q, index) => ({
      ...q,
      id: nextId + index
    })) as Question[];

    this.questions = [...this.questions, ...additionalQuestions];

  }

}
