import streamlit as st
import requests
import time
import json
from PIL import Image
import io
import base64

# Backend server URL
API_URL = "http://api_server:8000"

st.set_page_config(page_title="Distributed Traffic System", layout="centered")
st.title("🚦 Distributed Traffic Video Processing System")

st.markdown("""
Upload a traffic surveillance video, and the distributed system will:
- Detect Vehicles
- Extract License Plates
- Detect Helmet Violations
""")

uploaded_file = st.file_uploader("Upload a Traffic Video (.mp4)", type=["mp4"])

if uploaded_file is not None:
    if st.button("Submit Video"):
        st.success("Uploading video... Please wait.")

        files = {"file": (uploaded_file.name, uploaded_file, "video/mp4")}
        headers = {"X-API-Key": "traffic123"}

        try:
            response = requests.post(f"{API_URL}/upload/", files=files, headers=headers)
            response.raise_for_status()
            task_id = response.json()["task_id"]

            st.info(f"Video uploaded! Task ID: {task_id}")
            st.info("Processing video... This may take a minute ⏳")

            # Polling for result
            while True:
                res = requests.get(f"{API_URL}/result/{task_id}")
                res_data = res.json()

                if res_data["status"] == "done":
                    st.success("✅ Processing Complete!")
                    result = json.loads(res_data["result"])
                    break
                elif res_data["status"] == "not_found":
                    st.error("❌ Task Not Found.")
                    break
                else:
                    st.info("⏳ Still Processing...")
                    time.sleep(5)

            # Show result
            st.subheader("📊 Summary:")
            st.write(f"**Total Frames Processed**: {result['total_frames_processed']}")
            st.write(f"**Total Vehicles Detected**: {result['vehicle_count']}")
            st.write(f"**License Plates Found**: {len(result['license_plates'])}")
            st.write(f"**Helmet Violations Detected**: {len(result['helmet_violations'])}")

            st.subheader("🚗 Vehicle Types Detected:")
            for v in result["vehicles"]:
                st.write(f"- {v['type']}")

            st.subheader("🔵 License Plates:")
            for plate in result["license_plates"]:
                st.write(f"- {plate}")

            st.subheader("🔴 Helmet Violations:")
            for helmet in result["helmet_violations"]:
                st.write(f"- Frame {helmet['frame_no']} at Box {helmet['bbox']}")

            # Optional: Show annotated frames later
            st.info("📸 Frame visualization coming soon in next upgrade!")

        except Exception as e:
            st.error(f"Error uploading video: {e}")
