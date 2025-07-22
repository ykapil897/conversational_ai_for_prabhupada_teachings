import streamlit as st
import requests
import json
from datetime import datetime, timedelta
import pytz

# API_URL = "http://localhost:8000"  # Assuming FastAPI runs locally
API_URL = st.secrets["backend_url"]["url"]

st.set_page_config(page_title="Devotional Assistant", layout="wide")

if "user" not in st.session_state:
    st.session_state.user = None


IST = pytz.timezone("Asia/Kolkata")

def format_time_ist(iso_string):
    dt = datetime.fromisoformat(iso_string)
    dt_ist = dt + timedelta(hours=5, minutes=30)
    return dt_ist.strftime('%d %B %Y, %I:%M %p')


def show_onboarding():
    st.title("🕉️ Welcome to Your Devotional Assistant")
    with st.form("onboarding"):
        username = st.text_input("Enter your username")
        st.markdown("### Preferences")
        prabhupada_ratio = st.slider("Prabhupada Quote Ratio (%)", 0, 100, 70)
        answer_length = st.selectbox("Answer Length", ["short", "medium", "long"])
        answer_format = st.selectbox("Answer Format", ["conversational", "academic", "scriptural", "bullet-points", "lecture"])
        devotee_level = st.selectbox("Devotee Level", ["neophyte", "intermediate", "advanced"])
        quiz_type = st.selectbox("Preferred Quiz Type", ["fill-in-blank", "true-false", "mcq", "matching"])
        difficulty = st.selectbox("Default Quiz Difficulty", ["easy", "medium", "hard"])
        num_questions = st.slider("Default Number of Questions", 1, 10, 5)
        submitted = st.form_submit_button("Save & Continue")

        if submitted:
            prefs = {
                "username": username,
                "prabhupada_ratio": prabhupada_ratio,
                "answer_length": answer_length,
                "answer_format": answer_format,
                "devotee_level": devotee_level,
                "quiz_type": quiz_type,
                "difficulty": difficulty,
                "num_questions": num_questions
            }
            res = requests.post(f"{API_URL}/update_preferences", json=prefs)
            if res.ok:
                st.session_state.user = username
                st.success("Preferences saved successfully!")


def show_qa():
    st.title("🙏 Ask a Devotional Question")
    query = st.text_area("Your question here:")
    if st.button("Get Answer"):
        if query:
            res = requests.post(f"{API_URL}/process_query", json={"username": st.session_state.user, "query": query})
            if res.ok:
                st.markdown("### 📜 Answer")
                st.write(res.json()["answer"]["final_answer"])
            else:
                st.error("Something went wrong!")


def show_quiz():
    st.title("📘 Generate a Quiz")
    with st.form("quiz_form"):
        topic = st.text_input("Topic for Quiz (e.g. Chapter or Theme)")
        quiz_type = st.selectbox("Quiz Type", ["fill-in-blank", "true-false", "mcq", "matching"])
        difficulty = st.selectbox("Difficulty", ["easy", "medium", "hard"])
        num_questions = st.slider("Number of Questions", 1, 10, 5)
        submitted = st.form_submit_button("Generate")

        if submitted and topic:
            res = requests.post(f"{API_URL}/generate_quiz", json={
                "username": st.session_state.user,
                "topic": topic,
                "quiz_type": quiz_type,
                "difficulty": difficulty,
                "num_questions": num_questions
            })

            if res.ok:
                quiz = res.json()
                st.session_state.current_quiz = quiz
                st.session_state.quiz_answers = {}
                # st.experimental_rerun()

    if "current_quiz" in st.session_state:
        st.subheader("📋 Quiz Questions")
        quiz = st.session_state.current_quiz
        for i, q in enumerate(quiz.get("questions", []), 1):
            key = str(i)
            if quiz["meta"]["quiz_type"] == "true-false":
                st.session_state.quiz_answers[key] = st.radio(f"{i}. {q['statement']}", ["True", "False"], key=f"q_{i}")
            elif quiz["meta"]["quiz_type"] == "mcq":
                options = q["options"]
                values = list(options.values()) 
                st.session_state.quiz_answers[key] = st.radio(f"{i}. {q['question']}", values, key=f"q_{i}")
            else:
                st.session_state.quiz_answers[key] = st.text_input(f"{i}. {q.get('question', '')}", key=f"q_{i}")

        if st.button("Submit Quiz"):
            payload = {
                "username": st.session_state.user,
                "quiz_data": quiz,
                "user_answers": st.session_state.quiz_answers
            }
            result = requests.post(f"{API_URL}/submit_answers", json=payload)
            if result.ok:
                res = result.json()
                st.success(f"✅ You scored {res['correct_count']}/{res['total_questions']} ({res['score_percentage']:.2f}%)")


def show_history():
    st.title("📚 Your Activity History")
    res = requests.get(f"{API_URL}/get_history/{st.session_state.user}")
    if res.ok:
        history = res.json()

        with st.expander("🕉️ Queries Asked"):
            for i, q in enumerate(history["queries"], 1):
                st.markdown(f"### ✨ Query {i}")
                st.markdown(f"**Q:** {q['query']}")
                st.markdown(f"**A:** {q['answer']}")
                st.markdown(f"**Time:** {format_time_ist(q['timestamp'])}")
                st.divider()

        with st.expander("📘 Quizzes Taken"):
            for q in history["quizzes"]:
                st.divider()
                st.markdown(f"### 📝 Quiz Summary")
                st.markdown(f"**Type:** `{q['quiz_type']}` &nbsp;&nbsp;|&nbsp;&nbsp; **Difficulty:** `{q['difficulty']}` &nbsp;&nbsp;|&nbsp;&nbsp; **Score:** `{q['total_correct']}`")
                
                # Human-readable time
                st.markdown(f"**Time Taken:** {format_time_ist(q['timestamp'])}**")

                # Parse stored strings
                questions = json.loads(q["questions"])
                user_answers = json.loads(q["user_answers"])
                correct_answers = json.loads(q["correct_answers"])
                references = json.loads(q["references"])

                st.markdown("### 📘 Questions")
                for i, question in enumerate(questions):
                    st.markdown(f"**Q{i+1}:** {question}")
                    st.markdown(f"➡️ **Your Answer:** {user_answers[i]}")
                    st.markdown(f"✅ **Correct Answer:** {correct_answers[i]}")
                    st.markdown(f"📎 **Reference:** {references[i]}")
                    st.markdown("---")

        with st.expander("🔖 Verses Read"):
            for i, v in enumerate(history["verses"], 1):
                st.markdown(f"### 📖 Verse {i}")
                st.markdown(f"**Verse Content:** {v['verse']}")
                st.markdown(f"**Source:** {v['source']}")
                st.markdown(f"**Time Saved:** {format_time_ist(v['timestamp'])}")
                st.divider()


def show_preferences():
    st.title("⚙️ Edit Preferences")
    st.warning("Preferences can only be edited from the onboarding screen. Please restart app to reconfigure.")


# ---------------- Main Routing ---------------- #

if st.session_state.user is None:
    show_onboarding()

tabs = st.tabs(["🧠 Ask", "📘 Quiz", "📚 History", "⚙️ Preferences"])
with tabs[0]:
    show_qa()
with tabs[1]:
    show_quiz()
with tabs[2]:
    show_history()
with tabs[3]:
    show_preferences()
