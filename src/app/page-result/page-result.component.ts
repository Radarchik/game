import {Component, EventEmitter, OnInit, Output} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {BreakpointObserver, BreakpointState} from '@angular/cdk/layout';
import {Question} from '../model/question';
import { CommonModule } from '@angular/common';
import { MatTableModule } from '@angular/material/table';
import {MatTableDataSource} from '@angular/material/table';
import {QuestionsService} from '../questions.service';

@Component({
  selector: 'app-page-result',
  standalone: false,
  templateUrl: './page-result.component.html',
  styleUrls: ['./page-result.component.css']
})
export class PageResultComponent implements OnInit {
  @Output() pageToShowEvent = new EventEmitter<number>();
  private authToken = '76c030ab71daae5908437a13af8bec91';
  constructor(private httpClient: HttpClient,
              private questionsService: QuestionsService,
              private breakpointObserver: BreakpointObserver) { }

  questions: Question[] = [];

  displayedColumns: string[] = [
    'comment',
    'estimate',
    'text',
    'estimateAuthor',
    'commentAuthor'
  ];

  isMobile = false;

  ngOnInit(): void {
    this.breakpointObserver
      .observe(['(max-width: 768px)'])
      .subscribe((result: BreakpointState) => {
        this.isMobile = result.matches;
      });

    this.questionsService.questionsReplaySubject.subscribe(questions => {
      console.log(questions);
      this.questions = questions;
    });
  }



}
