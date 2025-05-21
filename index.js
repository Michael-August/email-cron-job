const express = require("express");
const dotenv = require("dotenv");
const { createClient } = require("redis");
const { SESClient, SendEmailCommand } = require("@aws-sdk/client-ses");
const moment = require("moment");

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

const BATCH_SIZE = 50;
const QUEUE_KEY = "subscribersQueue";

// Redis Setup
const redis = createClient({
	url: process.env.REDIS_URL,
});

redis.on("error", (err) => console.error("❌ Redis Error:", err));

(async () => {
	if (!redis.isOpen) await redis.connect();
})();

// AWS SES Setup
const region = process.env.REGION;
const accessKeyId = process.env.ACCESS_KEY_ID;
const secretAccessKey = process.env.SECRET_ACCESS_KEY;

if (!region || !accessKeyId || !secretAccessKey) {
	throw new Error("❌ AWS configuration missing");
}

const SESClientConfig = new SESClient({
	region,
	credentials: {
		accessKeyId,
		secretAccessKey,
	},
});

async function processEmailQueue(req, res) {
	const batch = await redis.lRange(QUEUE_KEY, 0, BATCH_SIZE - 1);
	if (batch.length === 0) {
		console.log("✅ No emails to process.");
		if (res) return res.json({ message: "No emails to process" });
		return;
	}

	const subscribers = batch.map((s) => JSON.parse(s));
	await sendEmailNotifications(
		subscribers,
		subscribers[0].articleData.content,
		subscribers[0].articleData.title,
		subscribers[0].articleData.slug
	);

	if (res) return res.json({ message: "Processed batch of emails" });
}

async function sendEmailNotifications(subscribers, content, title, slug) {
	const websiteUrl = "https://www.ewere.tech";
	const articleUrl = `${websiteUrl}/blog/${slug}`;
	const DELAY_BETWEEN_BATCHES = 10000;

	const sendPromises = subscribers.map((subscriber) => {
		if (!subscriber.email?.includes("@")) return;

		const emailParams = {
			Destination: { ToAddresses: [subscriber.email] },
			Message: {
				Body: {
					Html: {
						Charset: "UTF-8",
						Data: `
							<!DOCTYPE html>
							<html>
							<body>
								<h2>${title}</h2>
								<p>${content.slice(3, 300)}... <a href="${articleUrl}">Read more</a></p>
								<a href="${articleUrl}" style="padding:10px;background:black;color:white;text-decoration:none;">Read Now</a>
								<p><a href="http://ewere.tech/unsubscribe?email=${
									subscriber.email
								}">Unsubscribe</a></p>
							</body>
							</html>
						`,
					},
				},
				Subject: {
					Charset: "UTF-8",
					Data: `New Article Alert: ${title}`,
				},
			},
			Source: `Ewere Diagboya <${process.env.SES_VERIFIED_EMAIL}>`,
		};

		return SESClientConfig.send(new SendEmailCommand(emailParams));
	});

	await Promise.all(sendPromises);

	// Trim the queue
	await redis.lTrim(QUEUE_KEY, subscribers.length, -1);
	console.log(`✅ Removed ${subscribers.length} emails from queue`);

	await new Promise((resolve) => setTimeout(resolve, DELAY_BETWEEN_BATCHES));

	console.log(`✅ Sent ${subscribers.length} emails`);
}

// Create an HTTP endpoint for triggering
app.get("/", (req, res) => res.send("Email Queue Worker Running"));
app.get("/process-emails", processEmailQueue);

// Start the server
app.listen(PORT, () => {
	console.log(`🚀 Server running on port ${PORT}`);
});
