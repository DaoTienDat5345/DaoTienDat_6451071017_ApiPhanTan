require("dotenv").config();
const axios = require("axios");

const PAGE_ID = process.env.PAGE_ID;
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
const APP_ID = process.env.APP_ID;

async function subscribePage() {
  try {
    if (!PAGE_ID || !PAGE_ACCESS_TOKEN || !APP_ID) {
      throw new Error("Missing PAGE_ID, PAGE_ACCESS_TOKEN, or APP_ID in .env");
    }

    console.log(`--- Dang tien hanh ket noi App ${APP_ID} vao Page: ${PAGE_ID} ---`);

    const url = `https://graph.facebook.com/v25.0/${PAGE_ID}/subscribed_apps`;

    const response = await axios.post(
      url,
      {
        subscribed_fields: "feed",
        access_token: PAGE_ACCESS_TOKEN
      },
      {
        proxy: false
      }
    );

    if (response.data.success) {
      console.log("Ket noi thanh cong!");
      console.log("Du lieu phan hoi:", response.data);
      console.log("Tu bay gio, cac su kien tren Fanpage se duoc gui ve webhook cua ban.");
    } else {
      console.log("Phan hoi tu Graph API:", response.data);
    }
  } catch (error) {
    console.error("Loi ket noi:", error.response ? error.response.data : error.message);
  }
}

subscribePage();
