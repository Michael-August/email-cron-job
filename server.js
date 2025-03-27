const dotenv = require("dotenv");
const { createClient } = require("redis");
const { SESClient, SendEmailCommand } = require("@aws-sdk/client-ses");
const moment = require("moment");
const cron = require("node-cron");

// Load environment variables
dotenv.config();

dotenv.config();

const BATCH_SIZE = 50;
const PROCESS_INTERVAL = "*/30 * * * * *"; // Every 30 seconds
const QUEUE_KEY = "subscribersQueue";

// Initialize Redis
const redis = createClient({
	url: process.env.REDIS_URL,
});

redis.on("error", (err) => console.error("❌ Redis Error:", err));

(async () => {
	if (!redis.isOpen) await redis.connect(); // Ensure connection before use
})();

// AWS SES Setup
const region = process.env.REGION;
const accessKeyId = process.env.ACCESS_KEY_ID;
const secretAccessKey = process.env.SECRET_ACCESS_KEY;

if (!region || !accessKeyId || !secretAccessKey) {
	throw new Error("AWS configuration is missing environment variables");
}

const SESClientConfig = new SESClient({
	region,
	credentials: {
		accessKeyId,
		secretAccessKey,
	},
});

async function processEmailQueue() {
	const batch = await redis.lRange(QUEUE_KEY, 0, BATCH_SIZE - 1); // Get first 50
	if (batch.length === 0) {
		console.log("✅ No emails to process.");
		return;
	}

	const subscribers = batch.map((s) => JSON.parse(s));

	sendEmailNotifications(
		subscribers,
		subscribers[0].articleData.content,
		subscribers[0].articleData.title,
		subscribers[0].articleData.slug
	);
}

async function sendEmailNotifications(subscribers, content, title, slug) {
	const websiteUrl = "https://www.ewere.tech";
	const articleUrl = `${websiteUrl}/blog/${slug}`;

	const DELAY_BETWEEN_BATCHES = 10000; // 10 seconds delay

	let totalEmails = 0;

	try {
		const sendPromises = subscribers.map((subscriber) => {
			if (!subscriber.email || !subscriber.email.includes("@")) {
				console.error(`🚨 Invalid email detected: ${subscriber.email}`);
				return Promise.resolve(); // Skip this email
			}

			try {
				const emailParams = {
					Destination: { ToAddresses: [subscriber.email] },
					Message: {
						Body: {
							Html: {
								Charset: "UTF-8",
								Data: `
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <style>
                        body {
                            font-family: Arial, sans-serif;
                            margin: 0;
                            padding: 0;
                        }
                        .container { max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }
                        .button { display: inline-block; padding: 10px 20px; background: black; color: white !important; text-decoration: none; border-radius: 5px; margin-bottom: 20px; }
                        .privacy { margin-top: 10px; color: white !important; font-size: 12px; }
                        .footer {
                            text-align: center;
                            padding: 15px 0;
                            border-top: 1px solid #333;
                            font-size: 14px;
                            color: #bbb;
                            background: black;
                        }
                        </style>
                    </head>
                    <body>
                        <div class="container">
                        <p>Hey ${subscriber.fullName},</p>
                        <p>We've just published a new article that you might be interested in!</p>
                        <h2>${title}</h2>
                        <p>${content.slice(
							3,
							300
						)}... <a href="${articleUrl}">Read more</a></p>
                        <a href="${articleUrl}" class="button">Read Now</a>
                        <div class="footer">
                            <p>&copy; ${moment().year()} Ewere.tech. All Rights Reserved.</p>
                            <div class='privacy'>
                            <a href="${websiteUrl}/terms">Terms of Service</a> | <a href="${websiteUrl}/privacy">Privacy Policy</a>
                            </div>
                            <p><a href="http://ewere.tech/unsubscribe?email=${
								subscriber.email
							}" class="unsubscribe">Unsubscribe</a></p>
                        </div>
                        </div>
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
			} catch (error) {
				console.error(
					`❌ Failed to send email to ${subscriber.email}:`,
					error
				);
			}
		});

		// Send batch emails in parallel
		await Promise.all(sendPromises);

		// Remove processed emails
		await redis.lTrim(QUEUE_KEY, subscribers.length, -1);
		console.log(
			`✅ Removed ${subscribers.length} emails from queue: ${QUEUE_KEY}`
		);

		totalEmails += subscribers.length;
	} catch (error) {
		console.error("❌ Batch email sending failed:", error);
	} finally {
		console.log(
			`⏳ Waiting ${
				DELAY_BETWEEN_BATCHES / 1000
			}s before the next batch...`
		);
		await new Promise((resolve) =>
			setTimeout(resolve, DELAY_BETWEEN_BATCHES)
		);
	}

	console.log(`✅ Sent ${totalEmails} emails to subscribers`);
}

cron.schedule(PROCESS_INTERVAL, processEmailQueue);
