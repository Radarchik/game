import {Component, EventEmitter, Input, OnDestroy, OnInit, Output} from '@angular/core';
import {AbstractControl, FormArray, FormBuilder, FormGroup, Validators} from '@angular/forms';
import {HttpClient} from '@angular/common/http';
import {Question} from './model/question';
import {QuestionsService} from './questions.service';

@Component({
  selector: 'app-root',
  standalone: false,
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit, OnDestroy {
  public title = 'Анкета раба';
  startForm: FormGroup;
  questions: Question[];
  rightPassword;
  public pageToShow: number;

  constructor(private httpClient: HttpClient,
              private questionsService: QuestionsService,
              private formBuilder: FormBuilder) {
  }

  ngOnInit(): void {
    this.rightPassword = new Date().getDate() * 777;
    this.pageToShow = 0;
    this.startForm = this.formBuilder.group({
      password: ['', this.passWordValidator]
    });

    this.httpClient.get('assets/test.json', {responseType: 'json'})
      .subscribe(data => {
        this.questions = [];

        for (let i = 0; i < (data as Array<string>).length; i++) {
          const question = new Question();
          question.id = i + 1;
          question.category = data[i].category;
          question.text = data[i].name;
          question.estimateAuthor = data[i].estimateAuthor;
          question.commentAuthor = data[i].commentAuthor;
          question.isSituation = data[i].isSituation;
          question.isOwn = false;
          this.questions.push(question);
        }

        //перемешиваем вопросы
        this.shuffleQuestions();

        // добавляем собственные вопросы
        const len = this.questions.length + 1;
        for (let i = len; i < len + 3; i++) {
          const question = new Question();
          question.id = i;
          question.category = 'Собственные фантазии (опционально)';
          question.isOwn = true;
          question.text = '';
          this.questions.push(question);
        }
      });


    this.httpClient.post('https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net', 'Я зашла (Ксения)')
      .subscribe(response => {
        console.log('');
      });
  }

  pageEventHandler($event: any) {
    this.pageToShow = $event;
  }

  ngOnDestroy(): void {
  }


  goToPage1() {
    this.questionsService.questionsReplaySubject.next(this.questions);
    if (this.startForm.get('password').value === this.rightPassword.toString()) {
      this.pageToShow = 1;
    }
    if (this.startForm.get('password').value === '1311') {
      this.pageToShow = -1;
    }
    this.pageToShow = 1;
  }

  passWordValidator(control: AbstractControl) {
    if (control.value === (new Date().getDate() * 777).toString()) {
      return {wrongPassword: true};
    }
    return null;
  }


  shuffleQuestions(): void {
    for (let i = this.questions.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [this.questions[i], this.questions[j]] = [this.questions[j], this.questions[i]];
    }
  }
}
