module.exports = {
  plugins: ["import"],
  extends: ["airbnb-typescript/base", "prettier"],
  env: {
      node: true,
      browser: false,
      jest: false,
      mocha: true,
  },
  parserOptions: {
      project: "./tsconfig.json",
      tsconfigRootDir: __dirname,
  },
  rules: {
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/no-import-type-side-effects": "error",
      "import/extensions": "off",
      // Too restrictive, writing ugly code to defend against a very unlikely scenario: https://eslint.org/docs/rules/no-prototype-builtins
      "no-prototype-builtins": "off",
      // https://basarat.gitbooks.io/typescript/docs/tips/defaultIsBad.html
      "import/prefer-default-export": "off",
      "import/no-default-export": "error",
      // Too restrictive: https://github.com/yannickcr/eslint-plugin-react/blob/master/docs/rules/destructuring-assignment.md
      "react/destructuring-assignment": "off",
      // No jsx extension: https://github.com/facebook/create-react-app/issues/87#issuecomment-234627904
      "react/jsx-filename-extension": "off",
      // Use function hoisting to improve code readability
      "no-use-before-define": ["error", { functions: false, classes: true, variables: true }],
      // Makes no sense to allow type inferrence for expression parameters, but require typing the response
      "@typescript-eslint/explicit-function-return-type": [
          "error",
          { allowExpressions: true, allowTypedFunctionExpressions: true },
      ],
      "@typescript-eslint/no-use-before-define": [
          "error",
          { functions: false, classes: true, variables: true, typedefs: true },
      ],
      "class-methods-use-this": "off",
      "@typescript-eslint/explicit-function-return-type": "off",
      "@typescript-eslint/lines-between-class-members": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "consistent-return": "off",
  },
};
