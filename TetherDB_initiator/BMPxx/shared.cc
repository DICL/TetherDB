#include "BMPxx/bmpxx.h"
#include <cstdint>
#include <cstring>
#include <vector>
#include <stdexcept>
#include <bit>

namespace bmpxx
{
  bmp::DibHeaderMeta bmp::createDIBHeaderMeta(Dib56Header *dib_header)
  {
    auto dib_header_meta = DibHeaderMeta();

    // Padding rows
    const uint32_t row_width_bits = dib_header->width * dib_header->bits_per_pixel;
    const uint32_t row_padding_bits = (32 - (row_width_bits % 32)) % 32;
    const uint32_t padded_row_width_bytes = (row_width_bits + row_padding_bits) / 8;

    dib_header_meta.padded_row_width = padded_row_width_bytes;
    // Alpha
    dib_header_meta.has_alpha_channel = dib_header->masks_rgba.alpha_mask != 0;

    return dib_header_meta;
  }

  std::pair<std::vector<uint8_t>, BmpDesc> bmp::crop(std::vector<uint8_t> inputImage, BmpDesc desc, int x1, int y1, int x2, int y2) {
    int ch = desc.channels;
    int crop_width = x2 - x1, crop_height = y2 - y1;
    int width = desc.width, height = desc.height;

    std::vector<uint8_t> r_img(crop_width * crop_height * ch);
    bmpxx::BmpDesc r_desc(crop_width, crop_height, ch);
    int pos = 0;

    for (int j = 0; j < crop_height; j++) {
      for (int i = 0; i < crop_width; i++) {
        if ((x1 + i > width) || (y1 + j) > height)
          for (int k = 0; k < ch; k++)
            r_img[pos++] = 0;
        else
          for (int k = 0; k < ch; k++)
            r_img[pos++] = inputImage[(ch * width * (y1 + j)) + (ch * (x1 + i)) + k];
      }
    }
    return std::make_pair(r_img, r_desc);
  }

  std::vector<uint8_t> bmp::rotate(std::vector<uint8_t> inputImage, BmpDesc desc, double degree) {
    int width = desc.width, height = desc.height;
    int ch = desc.channels;
    int x = 0, y = 0;
    int dif = 0;
    double radian = degree * M_PI / 180.0;
    double cos_seta = cos(radian), sin_seta = sin(-radian);
    double center_x = (double)width / 2.0, center_y = (double)height / 2.0;

    std::vector<uint8_t> r_img(width * height * ch);
    int pos = 0;

    for (int j = 0; j < height; j++) {
      for (int i = 0; i < width; i++) {
        for (int k = 0; k < ch; k++) {
          x = (int)(center_x + ((double)j - center_y) * sin_seta + ((double)i - center_x) * cos_seta);
          y = (int)(center_y + ((double)j - center_y) * cos_seta - ((double)i - center_x) * sin_seta);

          dif = 0;
          if ((y >= 0 && y < height) && (x >= 0 && x < width)) {
            dif = inputImage[(ch * width * y) + (ch * x) + k];
          }
          r_img[pos++] = dif;
        }
      }
    }
    return r_img;
  }
}
