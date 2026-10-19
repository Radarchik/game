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
    this.resetOrder();
    const commonQuestions = this.questions.filter(q => q.isOwn === false);
    const ownQuestions = this.questions.filter(q => q.isOwn);
    const canContinue = commonQuestions.every(question => (question.estimate || question.answer)) && ownQuestions.every(q => q.text);
    if (true) {
      this.questionsService.questionsReplaySubject.next(this.questions);
      const body = JSON.stringify(this.questions);
      this.isLoad = true
      this.httpClient.post('https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net', body)
        .subscribe(response => {
          this.isLoad = false
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
  }
}
