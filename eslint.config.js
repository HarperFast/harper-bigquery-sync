import harperConfig from '@harperdb/code-guidelines/eslint';

export default [
	...harperConfig,
	// Custom configuration for BigQuery sync plugin
	{
		ignores: ['dist/', 'node_modules/', 'coverage/', 'ext/maritime-data-synthesizer/**', 'examples/**'],
	},
	{
		rules: {
			// Allow unused vars that start with underscore (intentional unused)
			'no-unused-vars': [
				'error',
				{
					argsIgnorePattern: '^_',
					varsIgnorePattern: '^_',
				},
			],
			// Allow unused function parameters (common in callbacks)
			'@typescript-eslint/no-unused-vars': 'off',
		},
	},
];

