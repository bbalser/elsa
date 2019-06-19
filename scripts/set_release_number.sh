#! /bin/bash
current_version=$(mix hex.build | grep 'Version:' | awk '{print $2}')
echo "Current Application Version is: " $current_version
read -p "Enter new version number: " new_version
files=(
  mix.exs
  README.md
)
for file in "${files[@]}"; do
  sed -i '' "s/$current_version/$new_version/g" "$file"
done

echo "You will need commit the changed files"
