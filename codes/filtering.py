import os

# Configuration
# Assuming files are named 's24_2018.vrt', 's24_2019.vrt', etc. 
years = [2018, 2019, 2020, 2021, 2022, 2023]
output_file = 's24_kasvis_merged.vrt'
keyword = 'kasvis'


def filter_and_merge():
    print(f"Starting extraction for keyword: '{keyword}'...")
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        # Write the required VRT positional attributes header once at the top
        header = "<" + "!-- #vrt positional-attributes: word ref lemma lemmacomp pos msd dephead deprel spaces initid lex/ nertag2 nertags2/ nerbio2 --" + ">\n"
        outfile.write(header)
        
        for year in years:
            input_file = f's24_{year}.vrt'
            
            if not os.path.exists(input_file):
                print(f"Warning: {input_file} not found. Skipping.")
                continue
                
            print(f"Processing {input_file}...")
            
            with open(input_file, 'r', encoding='utf-8') as infile:
                in_text_block = False
                current_text_buffer = []
                keep_text = False
                
                for line in infile:
                    # Skip existing VRT attribute comments using a workaround so it doesn't break chat UI
                    comment_tag = "<" + "!-- #vrt"
                    if line.startswith(comment_tag):
                        continue
                        
                    # Detect the start of a message
                    if line.startswith('<text'):
                        in_text_block = True
                        current_text_buffer = [line]
                        keep_text = False
                        
                    # Detect the end of a message
                    elif line.startswith('</text>'):
                        current_text_buffer.append(line)
                        # If the keyword was found anywhere in this message, write the whole block
                        if keep_text:
                            outfile.writelines(current_text_buffer)
                            
                        # Reset for the next message
                        in_text_block = False
                        current_text_buffer = []
                        
                    # Inside a message block
                    elif in_text_block:
                        current_text_buffer.append(line)
                        # Check for the keyword (case-insensitive)
                        if not keep_text and keyword in line.lower():
                            keep_text = True

    print(f"Filtering complete! Saved to {output_file}")

if __name__ == "__main__":
    filter_and_merge()
    