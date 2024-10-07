import java.sql.*;

public class Main {
    private static final String URL = "jdbc:postgresql://localhost:5432/Lb_10";
    private static final String USER = "postgres";
    private static final String PASSWORD = "awerta357";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            createTables(conn);

            addStudent(conn, "���������� �����", "1534", "662890");
            addStudent(conn, "���� �����", "1634", "567734");
            addStudent(conn, "����� �������", "7238", "583659");
            addStudent(conn, "������ ��������", "3421", "464242");
            addSubject(conn, "����������");
            addSubject(conn, "�����������");
            addSubject(conn, "�����");
            addSubject(conn, "������� ����");
            addProgress(conn, 1, 1, 4);
            addProgress(conn, 1, 2, 4);
            addProgress(conn, 1, 3, 5);
            addProgress(conn, 2, 1, 4);
            addProgress(conn, 2, 2, 4);
            addProgress(conn, 2, 3, 3);
            addProgress(conn, 2, 4, 2);
            addProgress(conn, 3, 1, 5);
            addProgress(conn, 3, 2, 5);
            addProgress(conn, 3, 3, 4);
            addProgress(conn, 3, 4, 3);
            addProgress(conn, 4, 1, 4);
            addProgress(conn, 4, 2, 5);
            addProgress(conn, 4, 3, 5);
            addProgress(conn, 4, 4, 4);

            System.out.println("������ ���������, ������� ����������� ����������:");
            listStudentsWithGradeAbove3(conn, "�����");
            calculateAverageGradeForSubject(conn, "����������");
            calculateAverageGradeForStudent(conn, "������ ��������");
            System.out.println("��� ����������, � �������� ���������� ����������� ���������:");
            findTop3Subjects(conn);
            listStudentsOnScholarship(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ����� ��� �������� ������ ���������, ��������� � ������������
    private static void createTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // ������� ��������� (������� 1)
            stmt.execute("CREATE TABLE IF NOT EXISTS students (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(100) NOT NULL, " +
                    "passport_series VARCHAR(10) NOT NULL, " +
                    "passport_number VARCHAR(10) NOT NULL, " +
                    "UNIQUE (passport_series, passport_number))");  // ������� 9: ���������� ���� �����-����� ��������

            // ������� ������� ��������� (������� 2)
            stmt.execute("CREATE TABLE IF NOT EXISTS subjects (" +
                    "id SERIAL PRIMARY KEY, " +
                    "subject_name VARCHAR(100) NOT NULL)");

            // ������� ������� ������������ (������� 3)
            stmt.execute("CREATE TABLE IF NOT EXISTS progress (" +
                    "id SERIAL PRIMARY KEY, " +
                    "student_id INT REFERENCES students(id) ON DELETE CASCADE, " +  // ������� 6: �������� ���� ������� ��� �������� ��������
                    "subject_id INT REFERENCES subjects(id), " +
                    "grade INT CHECK (grade BETWEEN 2 AND 5))");  // ������� 4: ������ � �������� �� 2 �� 5
        }
    }

    // ������� 5: ������� ������ ���������, ������� ������������ ������� �� ������ ���� 3
    // ������� 5: ������� ������ ���������, ������� ������������ ������� �� ������ ���� 3
    private static void listStudentsWithGradeAbove3(Connection conn, String subjectName) throws SQLException {
        String query = "SELECT s.name FROM students s " +
                "JOIN progress p ON s.id = p.student_id " +
                "JOIN subjects sub ON p.subject_id = sub.id " +
                "WHERE sub.subject_name = ? AND p.grade > 3";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, subjectName);
            ResultSet rs = pstmt.executeQuery();
            System.out.println("����������: " + subjectName); // ������� �������� ����������
            while (rs.next()) {
                System.out.println("�������: " + rs.getString("name"));
            }
        }
    }


    // ������� 7: ��������� ������� ���� �� ������������� ��������
    private static void calculateAverageGradeForSubject(Connection conn, String subjectName) throws SQLException {
        String query = "SELECT AVG(p.grade) as average_grade " +
                "FROM progress p " +
                "JOIN subjects sub ON p.subject_id = sub.id " +
                "WHERE sub.subject_name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, subjectName);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // ������� ������� ����
                System.out.println("������� ���� ���������� - " + subjectName + ": " + rs.getDouble("average_grade"));
            }
        }
    }

    // ������� 8: ��������� ������� ���� �� ������������� ��������
    private static void calculateAverageGradeForStudent(Connection conn, String studentName) throws SQLException {
        String query = "SELECT AVG(p.grade) as average_grade " +
                "FROM progress p " +
                "JOIN students s ON p.student_id = s.id " +
                "WHERE s.name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, studentName);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.println("������� ���� �������� - " + studentName + ": " + rs.getDouble("average_grade"));
            }
        }
    }

    // ������� 10: ����� ��� ��������, ������� ����� ���������� ���������� ���������
    private static void findTop3Subjects(Connection conn) throws SQLException {
        String query = "SELECT sub.subject_name, COUNT(p.student_id) AS student_count " +
                "FROM progress p " +
                "JOIN subjects sub ON p.subject_id = sub.id " +
                "WHERE p.grade > 2 " +  // ������� ������ ������ ������ 2
                "GROUP BY sub.subject_name " +
                "ORDER BY student_count DESC " +
                "LIMIT 3";  // ������������ ����� �� 3 ���������

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.println("����������: " + rs.getString("subject_name") + ", ��������� �����: " + rs.getInt("student_count"));
            }
        }
    }


    private static void addStudent(Connection conn, String name, String passportSeries, String passportNumber) throws SQLException {
        String query = "INSERT INTO students (name, passport_series, passport_number) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, name);
            pstmt.setString(2, passportSeries);
            pstmt.setString(3, passportNumber);
            pstmt.executeUpdate();
        }
    }
    private static void addSubject(Connection conn, String subjectName) throws SQLException {
        String query = "INSERT INTO subjects (subject_name) VALUES (?)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, subjectName);
            pstmt.executeUpdate();
        }
    }
    private static void addProgress(Connection conn, int studentId, int subjectId, int grade) throws SQLException {
        String query = "INSERT INTO progress (student_id, subject_id, grade) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, studentId);
            pstmt.setInt(2, subjectId);
            pstmt.setInt(3, grade);
            pstmt.executeUpdate();
        }
    }

    private static void listStudentsOnScholarship(Connection conn) throws SQLException{
        String query = "SELECT s.name FROM students s " +
                "WHERE NOT EXISTS ( " +
                "   SELECT 1 FROM progress p " +
                "   JOIN subjects sub ON p.subject_id = sub.id " +
                "   WHERE p.student_id = s.id AND (p.grade <= 3 OR p.grade IS NULL) " +  // ��� ����� � ������
                ") AND ( " +
                "   SELECT COUNT(*) FROM subjects " +
                ") = ( " +
                "   SELECT COUNT(*) FROM progress p " +
                "   WHERE p.student_id = s.id " +
                ")"; // ��� �������� �����
        try (PreparedStatement pstmt = conn.prepareStatement(query)){
            ResultSet rs = pstmt.executeQuery();
            System.out.println("��������, ������ �� ���������:");
            while (rs.next()) {
                System.out.println("�������: " + rs.getString("name"));
            }
        }
    }
}
