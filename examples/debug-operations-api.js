/**
 * Debug script to understand Harper Operations API format
 */

async function testCreateTable() {
	const baseUrl = 'http://localhost:9925';
	const username = 'admin';
	const password = 'abc123';

	const credentials = Buffer.from(`${username}:${password}`).toString('base64');

	// Test 1: Minimal create_table request
	console.log('Test 1: Minimal create_table');
	const minimalPayload = {
		operation: 'create_table',
		table: 'MinimalTest',
		hash_attribute: 'id',
	};

	console.log('Sending:', JSON.stringify(minimalPayload, null, 2));

	try {
		const response = await fetch(baseUrl, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Basic ${credentials}`,
			},
			body: JSON.stringify(minimalPayload),
		});

		const text = await response.text();
		console.log('Response status:', response.status);
		console.log('Response:', text);
		console.log();
	} catch (error) {
		console.error('Error:', error.message);
	}

	// Test 2: With schema field
	console.log('Test 2: With schema field');
	const schemaPayload = {
		operation: 'create_table',
		table: 'SchemaTest',
		hash_attribute: 'id',
		schema: {
			id: { type: 'string' },
			name: { type: 'string' },
		},
	};

	console.log('Sending:', JSON.stringify(schemaPayload, null, 2));

	try {
		const response = await fetch(baseUrl, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Basic ${credentials}`,
			},
			body: JSON.stringify(schemaPayload),
		});

		const text = await response.text();
		console.log('Response status:', response.status);
		console.log('Response:', text);
		console.log();
	} catch (error) {
		console.error('Error:', error.message);
	}

	// Test 3: describe_table to see what format Harper uses
	console.log('Test 3: Describe existing table (if any)');
	const describePayload = {
		operation: 'describe_table',
		table: 'SyncCheckpoint', // From our schema
	};

	console.log('Sending:', JSON.stringify(describePayload, null, 2));

	try {
		const response = await fetch(baseUrl, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Basic ${credentials}`,
			},
			body: JSON.stringify(describePayload),
		});

		const text = await response.text();
		console.log('Response status:', response.status);
		console.log('Response:', text);
		console.log();
	} catch (error) {
		console.error('Error:', error.message);
	}
}

testCreateTable();
