package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(constr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), constr)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к БД: %w", err)
	}
	return &Storage{db: db}, nil
}

type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `SELECT * FROM tasks WHERE ($1 = 0 OR id = $1) AND ($2 = 0 OR author_id = $2) ORDER BY id;`, taskID, authorID)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения задач: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Opened, &t.Closed, &t.AuthorID, &t.AssignedID, &t.Title, &t.Content); err != nil {
			return nil, fmt.Errorf("ошибка сканирования задачи: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *Storage) Mark(labelID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `SELECT t.id, t.opened, t.closed, t.author_id, t.assigned_id, t.title, t.content 
		FROM tasks t JOIN task_labels tl ON t.id = tl.task_id WHERE tl.label_id = $1;`, labelID)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения задач по метке: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Opened, &t.Closed, &t.AuthorID, &t.AssignedID, &t.Title, &t.Content); err != nil {
			return nil, fmt.Errorf("ошибка сканирования задачи: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `INSERT INTO tasks (title, content) VALUES ($1, $2) RETURNING id;`, t.Title, t.Content).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ошибка при создании задачи: %w", err)
	}
	return id, nil
}

func (s *Storage) Update(t Task) error {
	_, err := s.db.Exec(context.Background(), `UPDATE tasks SET title = $1, content = $2, assigned_id = $3, closed = $4 WHERE id = $5;`, t.Title, t.Content, t.AssignedID, t.Closed, t.ID)
	if err != nil {
		return fmt.Errorf("ошибка обновления задачи: %w", err)
	}
	return nil
}

func (s *Storage) Delete(taskID int) error {
	_, err := s.db.Exec(context.Background(), `DELETE FROM tasks WHERE id = $1;`, taskID)
	if err != nil {
		return fmt.Errorf("ошибка удаления задачи: %w", err)
	}
	return nil
}
