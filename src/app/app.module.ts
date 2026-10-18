import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';

import { AppComponent } from './app.component';
import { CustomMaterialModule } from './custom-material.module';
import { Page1Component } from './page1/page1.component';
import { Page2Component } from './page2/page2.component';
import { CommentDialogComponent } from './comment-dialog/comment-dialog.component';
import { PageEndComponent } from './page-end/page-end.component';
import { PageResultComponent } from './page-result/page-result.component';

import { MatDialogModule } from '@angular/material/dialog';
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatTableModule } from '@angular/material/table';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatInputModule } from '@angular/material/input';
import {MatButtonToggle, MatButtonToggleGroup} from '@angular/material/button-toggle';

@NgModule({
  declarations: [
    AppComponent,
    Page1Component,
    Page2Component,
    CommentDialogComponent,
    PageEndComponent,
    PageResultComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    CustomMaterialModule,
    MatDialogModule,
    MatPaginatorModule,
    MatTableModule,
    MatIconModule,
    MatButtonModule,
    MatInputModule,
    MatButtonToggleGroup,
    MatButtonToggle
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule {}
