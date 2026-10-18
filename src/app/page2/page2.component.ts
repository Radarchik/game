import {AfterViewInit, Component, EventEmitter, OnInit, Output, ViewChild, ViewEncapsulation} from '@angular/core';
import {MatDialog} from '@angular/material/dialog';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import {MatTableDataSource} from '@angular/material/table';
import { HttpClient } from '@angular/common/http';
import {Question} from '../model/question';
import {MatSnackBar} from '@angular/material/snack-bar';
import {CommentDialogComponent} from '../comment-dialog/comment-dialog.component';
import {QuestionsService} from '../questions.service';

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

  displayedColumns: string[] = ['number', 'name', 'estimate', 'comment'];

  constructor(
    public dialog: MatDialog,
    private snackBar: MatSnackBar,
    private httpClient: HttpClient,
    private questionsService: QuestionsService) { }

  @ViewChild(MatPaginator, {static: false}) paginator: MatPaginator;
  isMobile: boolean;

  ngAfterViewInit() {
  }

  ngOnInit() {
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
    const ownQuestions = this.questions.filter(q => q.isOwn);
    if (commonQuestions.every(question => (question.estimate || question.answer)) && ownQuestions.every(q => q.text)) {
      const body = JSON.stringify(this.questions);
      this.httpClient.post('https://c0d1a05f981d1b15d231e9a86e2ce39f.m.pipedream.net', body)
        .subscribe(response => {
          const message = 'Анкету получила';
          const action = 'Успешно!';
          if (response) {
            this.snackBar.open(message, action, {
              duration: 15000,
            });
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
    const unsignedQ = this.questions.filter(question => (!question.isOwn && (!question.estimate && !question.answer)) ||
    question.isOwn && question.text === '');
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
}
